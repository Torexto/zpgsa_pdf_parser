use crate::utility::filesystem::save_output;
use crate::{OperatingDays, SchoolRestriction, StopDetailsBus};
use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn suffix_parse(
    bus: &str,
    destination_map: &HashMap<String, String>,
    line_name: &str,
    line_number: &str,
    operating_days: OperatingDays,
) -> StopDetailsBus {
    let re = unsafe { Regex::new(r"(?P<time>\d{1,2}:\d{2})(?P<suffix>[A-Z]*)").unwrap_unchecked() };

    let caps = re.captures(bus.trim()).unwrap();

    let mut time = caps.name("time").unwrap().as_str().to_string();
    let suffix = caps.name("suffix").unwrap().as_str();

    let destination = match suffix.chars().next() {
        Some(suffix) => match destination_map.get(suffix.to_string().as_str()) {
            Some(destination) => destination.to_string(),
            None => line_name.to_string(),
        },
        None => line_name.to_string(),
    };

    let school_restriction = match suffix.chars().last() {
        Some('S') => SchoolRestriction::SchoolOnly,
        Some('W') => SchoolRestriction::FreeDayOnly,
        _ => SchoolRestriction::Normal,
    };

    if time.len() < 5 {
        time = format!("0{time}");
    }

    StopDetailsBus {
        time: time.to_string(),
        line: line_number.to_string(),
        destination: destination.to_string(),
        operating_days,
        school_restriction,
    }
}

/// Sprowadza identyfikator przystanku z nagłówka PDF do postaci kanonicznej.
///
/// Ta sama funkcja jest używana przy ładowaniu backupu, żeby oba źródła danych
/// były kluczowane identycznie (patrz `load_backup`).
pub fn normalize_stop_id<'a>(id: &'a str, destination: &str) -> &'a str {
    match id {
        "337-338" => match destination {
            "Dzierżoniów Piłsudskiego" => "338",
            "Książnica 27" => "337",
            _ => id,
        },
        "221" => match destination {
            "Dobrocin Szkoła" => "5005",
            _ => id,
        },
        "Handlowy" => "5002",
        "Szkoła" => "5006",
        "284." => "284",
        "45." => "45",
        "I" => "5009",
        "Kościół" => "5011",
        "51,53" => "51",
        "999" => "84",
        "14-15" => "14",
        "352-353" => "352",
        _ => id,
    }
}

fn destination_update(original_des: &'_ str) -> &'_ str {
    let des = original_des
        .strip_suffix(". Nie kursuje")
        .unwrap_or(original_des);
    match des {
        "Dzierżoniów Dzierżoniów dworzec  PKP" => "Dzierżoniów Dworzec PKP",
        "Dzierżoniów Dzierżoniów dworzec PKP" => "Dzierżoniów Dworzec PKP",
        "Dzierżoniów dworzec  PKP" => "Dzierżoniów Dworzec PKP",
        "Dzierżoniów dworzec PKP" => "Dzierżoniów Dworzec PKP",
        "Niemcza dworzec PKP" => "Niemcza Dworzec PKP",
        "Jodłownik Jodłownik" => "Jodłownik",
        "Dzierżoniów  Staszica. Nie kursuje" => "Dzierżoniów Staszica",
        "Byszów 221/81" | "Byszów 221" | "Byszów 221. Nie kursuje" => "Byszów 221",
        "Owiesno Kościół. Nie kursuje" => "Owiesno Kościół",
        "Dzierżoniów dworzec PKP. Nie kursuje" => "Dzierżoniów dworzec PKP",
        "Bielawa Camping Sudety. Nie kursuje" => "Bielawa Camping Sudety",
        _ => des,
    }
}

fn parse_line(line: &str, details: &mut HashMap<String, Vec<StopDetailsBus>>) {
    let mut stop_detail: Vec<StopDetailsBus> = Vec::new();

    let line_info_regex: Regex = unsafe {
        Regex::new(r"LINIA: (?<line_number>\S+) KIERUNEK: (?<destination>.+?) Przystanek: (?<stop>.*?) (?<id>\S+?) Czas").unwrap_unchecked()
    };

    let info = line_info_regex.captures(line).unwrap();

    let line_number = info.name("line_number").unwrap().as_str().trim();
    let destination = info.name("destination").unwrap().as_str().trim();
    let stop = info.name("stop").unwrap().as_str().trim();
    let id = info.name("id").unwrap().as_str().trim();

    let destination = destination_update(destination);

    let id = match stop {
        "Jędrzejowice" => "5001",
        _ => id,
    };

    let id = normalize_stop_id(id, destination);

    let legend_regex = unsafe { Regex::new(r"Legenda:\s*(.*?)\s*Operator:").unwrap_unchecked() };

    let destinations_map = if let Some(caps) = legend_regex.captures(line) {
        let legend_text = caps.get(1).unwrap().as_str();
        let marker_re = unsafe { Regex::new(r"([A-Z])\s*-\s*").unwrap_unchecked() };
        let mut result = HashMap::new();

        let mut matches = marker_re.find_iter(legend_text).peekable();

        while let Some(current) = matches.next() {
            let key = &legend_text[current.start()..current.end()];
            let label = key.chars().next().unwrap().to_string();

            let value_start = current.end();
            let value_end = matches
                .peek()
                .map(|next| next.start())
                .unwrap_or(legend_text.len());
            let value = legend_text[value_start..value_end].trim();

            let reg = unsafe { Regex::new(r"Kurs do:\s*(.*?)(?:\s+przez|$)").unwrap_unchecked() };
            if let Some(v) = reg.captures(&value) {
                result.insert(
                    label,
                    destination_update(v.get(1).unwrap().as_str()).to_string(),
                );
            }
        }
        result
    } else {
        HashMap::new()
    };

    let work_days_regex =
        unsafe { Regex::new(r"Dni robocze((?: \d{1,2}:\d{2}[A-Z]{0,3})*)").unwrap_unchecked() };

    let saturday_regex =
        unsafe { Regex::new(r"Soboty((?: \d{1,2}:\d{2}[A-Z]{0,3})*)").unwrap_unchecked() };

    let sunday_regex = unsafe {
        Regex::new(r"Niedziele i święta((?: \d{1,2}:\d{2}[A-Z]{0,3})*)").unwrap_unchecked()
    };

    if let Some(work_days) = work_days_regex.captures(line) {
        let mut t: Vec<_> = work_days
            .get(1)
            .unwrap()
            .as_str()
            .trim()
            .split(" ")
            .map(|time| {
                suffix_parse(
                    time,
                    &destinations_map,
                    destination,
                    line_number,
                    OperatingDays::MondayToFriday,
                )
            })
            .collect();
        stop_detail.append(&mut t);
    }
    if let Some(saturday) = saturday_regex.captures(line) {
        let mut t: Vec<_> = saturday
            .get(1)
            .unwrap()
            .as_str()
            .trim()
            .split(" ")
            .map(|time| {
                suffix_parse(
                    time,
                    &destinations_map,
                    destination,
                    line_number,
                    OperatingDays::Saturday,
                )
            })
            .collect();
        stop_detail.append(&mut t);
    }
    if let Some(sunday) = sunday_regex.captures(line) {
        let mut t: Vec<_> = sunday
            .get(1)
            .unwrap()
            .as_str()
            .trim()
            .split(" ")
            .map(|time| {
                suffix_parse(
                    time,
                    &destinations_map,
                    destination,
                    line_number,
                    OperatingDays::Sunday,
                )
            })
            .collect();
        stop_detail.append(&mut t);
    }

    match details.get_mut(id) {
        Some(stop_details) => {
            stop_details.append(&mut stop_detail);
        }
        None => {
            details.insert(id.to_string(), stop_detail);
        }
    };
}

fn extract_text(pdf: &Path) -> HashMap<String, Vec<StopDetailsBus>> {
    let mut details: HashMap<String, Vec<StopDetailsBus>> = HashMap::new();

    if let Ok(text) = pdf_extract::extract_text(&pdf) {
        let text = text.split_whitespace().collect::<Vec<&str>>().join(" ");
        let lines: Vec<&str> = text
            .split("Organizator:ZPGSA, ul. Piastowska 19a, Tel: 74 832 87 78")
            .collect();

        for line in lines.iter().take(lines.len().saturating_sub(1)) {
            parse_line(line, &mut details);
        }
    }
    details
}

pub fn process_pdf(path: PathBuf) -> HashMap<String, Vec<StopDetailsBus>> {
    let file_stem = path.file_stem().unwrap().to_string_lossy();
    let output_json = Path::new("output").join(format!("{}.json", file_stem));

    let lines = extract_text(&path);
    save_output(&output_json, &lines).unwrap();
    lines
}
