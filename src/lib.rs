use crate::parser::check::check_pdf;
use crate::parser::parser::{normalize_stop_id, process_pdf};
use anyhow::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use utility::filesystem::clear_dir;
use utility::filesystem::load_timetable;
use utility::html::{download_pdf, get_pdf_list};
use walkdir::WalkDir;

mod parser;
mod utility;

pub use utility::filesystem::save_output;
pub use utility::filesystem::load_expected_counts;

const SOURCE: &str = "source";
const OUTPUT: &str = "output";
const TEMP: &str = "temp";


pub type StopCounts = HashMap<String, HashMap<String, HashMap<OperatingDays, usize>>>;

pub type Timetable = HashMap<String, Vec<StopDetailsBus>>;

/// Warianty `alias` obsługują starsze zapisy (m.in. `backup.json`); zapis
/// zawsze używa nazw kanonicznych.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, Hash, PartialEq)]
pub enum OperatingDays {
    #[serde(rename = "mon_fri", alias = "work")]
    MondayToFriday,
    #[serde(rename = "saturday", alias = "sat")]
    Saturday,
    #[serde(rename = "sunday", alias = "sun")]
    Sunday
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SchoolRestriction {
    #[serde(rename = "normal")]
    Normal,
    #[serde(rename = "school_only")]
    SchoolOnly,
    #[serde(rename = "free_day_only")]
    FreeDayOnly,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StopDetailsBus {
    pub time: String,
    pub line: String,
    pub destination: String,
    pub operating_days: OperatingDays,
    pub school_restriction: SchoolRestriction,
}

#[derive(Debug, Serialize, Clone)]
pub struct Mismatch {
    pub stop_id: String,
    pub line: String,
    pub day: OperatingDays,
    pub expected: usize,
    pub actual: usize,
}

/// Grupa odjazdów (przystanek + linia + typ dni), którą podmieniono danymi z backupu.
#[derive(Debug, Serialize, Clone)]
pub struct Repair {
    pub stop_id: String,
    pub line: String,
    pub day: OperatingDays,
    pub parsed: usize,
    pub restored: usize,
}

#[derive(Debug, Default)]
pub struct RepairReport {
    /// Grupy naprawione z backupu.
    pub repaired: Vec<Repair>,
    /// Grupy, których nie dało się naprawić – backup też nie zgadza się ze wzorcem.
    pub unresolved: Vec<Mismatch>,
}

pub async fn download_pdfs() -> Result<()> {
    clear_dir(SOURCE).await?;

    let raw_urls = get_pdf_list().await?;

    let tasks = raw_urls.iter().map(|url| download_pdf(url));
    let results = futures::future::join_all(tasks).await;

    for res in results {
        res?;
    }

    Ok(())
}

pub async fn clear_dirs() -> Result<()> {
    clear_dir(OUTPUT).await?;
    clear_dir(TEMP).await?;

    Ok(())
}

pub async fn scan_pdfs() -> Result<Vec<PathBuf>> {
    let pdfs: Vec<PathBuf> = WalkDir::new(SOURCE)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|f| match f.path().extension() {
            Some(file) => file == "pdf",
            None => false,
        })
        .map(|e| e.path().to_path_buf())
        .collect();
    Ok(pdfs)
}

pub async fn check_pdfs(pdf_paths: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    let pdfs: Vec<PathBuf> = pdf_paths
        .into_par_iter()
        .filter_map(|path| check_pdf(path).ok())
        .collect();

    Ok(pdfs)
}

pub fn process_pdfs(pdfs: Vec<PathBuf>) -> Timetable {
    let result: Timetable = pdfs.into_par_iter().flat_map(process_pdf).collect();

    result
}

/// Ładuje backup i sprowadza identyfikatory przystanków do postaci kanonicznej.
///
/// Backup jest zapisem wcześniejszego przebiegu, mógł więc powstać przed
/// dodaniem części reguł mapowania ID (np. `"Handlowy"` -> `"5002"`). Bez tego
/// kroku takie przystanki byłyby dla naprawy niewidoczne.
pub fn load_backup<P: AsRef<Path>>(path: P) -> Result<Timetable> {
    let raw = load_timetable(path)?;

    let mut backup: Timetable = HashMap::with_capacity(raw.len());

    for (stop_id, departures) in raw {
        for departure in departures {
            let id = normalize_stop_id(&stop_id, &departure.destination);

            backup.entry(id.to_string()).or_default().push(departure);
        }
    }

    Ok(backup)
}

/// Naprawia niepoprawne grupy odjazdów danymi z backupu.
///
/// Naprawa jest wykonywana per grupa (przystanek + linia + typ dni) i tylko
/// wtedy, gdy backup zgadza się ze wzorcem z `count.json` – dane z backupu są
/// starsze, więc przyjmujemy je jedynie po weryfikacji. Grupy, których backup
/// nie potwierdza, zostają nietknięte i trafiają do `unresolved`.
pub fn repair_from_backup(
    result: &mut Timetable,
    backup: &Timetable,
    mismatches: &[Mismatch],
) -> RepairReport {
    let mut report = RepairReport::default();

    for mismatch in mismatches {
        let in_group = |departure: &StopDetailsBus| {
            departure.line == mismatch.line && departure.operating_days == mismatch.day
        };

        let replacement: Vec<StopDetailsBus> = backup
            .get(&mismatch.stop_id)
            .map(|departures| departures.iter().filter(|d| in_group(d)).cloned().collect())
            .unwrap_or_default();

        if replacement.len() != mismatch.expected {
            report.unresolved.push(mismatch.clone());
            continue;
        }

        let restored = replacement.len();

        match result.get_mut(&mismatch.stop_id) {
            Some(departures) => {
                departures.retain(|d| !in_group(d));
                departures.extend(replacement);

                if departures.is_empty() {
                    result.remove(&mismatch.stop_id);
                }
            }
            None if restored > 0 => {
                result.insert(mismatch.stop_id.clone(), replacement);
            }
            None => {}
        }

        report.repaired.push(Repair {
            stop_id: mismatch.stop_id.clone(),
            line: mismatch.line.clone(),
            day: mismatch.day.clone(),
            parsed: mismatch.actual,
            restored,
        });
    }

    report
}


pub fn verify_counts(actual: &StopCounts, expected: &StopCounts) -> Vec<Mismatch> {
    let mut mismatches = Vec::new();

    // 1. Sprawdzamy wszystkie oczekiwane wpisy ze wzorca
    for (stop_id, exp_lines) in expected {
        let act_lines = actual.get(stop_id);

        for (line, exp_days) in exp_lines {
            let act_days = act_lines.and_then(|l| l.get(line));

            for (day, &exp_count) in exp_days {
                let act_count = act_days
                    .and_then(|d| d.get(day))
                    .copied()
                    .unwrap_or(0);

                if act_count != exp_count {
                    mismatches.push(Mismatch {
                        stop_id: stop_id.clone(),
                        line: line.clone(),
                        day: day.clone(),
                        expected: exp_count,
                        actual: act_count,
                    });
                }
            }
        }
    }

    // 2. Szukamy wpisów, które parser znalazł, a których nie ma we wzorcu
    for (stop_id, act_lines) in actual {
        for (line, act_days) in act_lines {
            for (day, &act_count) in act_days {
                let exp_count = expected
                    .get(stop_id)
                    .and_then(|l| l.get(line))
                    .and_then(|d| d.get(day))
                    .copied()
                    .unwrap_or(0);

                // Jeśli we wzorcu było 0, a parser coś znalazł i nie wyszczególniliśmy tego wcześniej
                if exp_count == 0 && act_count > 0 {
                    // Unikamy duplikatów, jeśli przypadek został już wyłapany wyżej
                    if !mismatches.iter().any(|m| m.stop_id == *stop_id && m.line == *line && m.day == *day) {
                        mismatches.push(Mismatch {
                            stop_id: stop_id.clone(),
                            line: line.clone(),
                            day: day.clone(),
                            expected: 0,
                            actual: act_count,
                        });
                    }
                }
            }
        }
    }

    mismatches
}

pub fn count_stops(result: &Timetable) -> StopCounts {
    let mut counts: StopCounts = HashMap::new();

    for (stop_id, stops) in result {
        let lines_map = counts.entry(stop_id.clone()).or_default();

        for stop in stops {
            lines_map
                .entry(stop.line.clone())
                .or_default()
                .entry(stop.operating_days.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }

    counts
}