use futures::future::join_all;
use kuchiki::parse_html;
use kuchiki::traits::TendrilSink;
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use std::{
    env,
    fs::{self, File},
    path::{Path, PathBuf},
    process::Command,
};
use walkdir::WalkDir;

const SOURCE: &str = "source";
const OUTPUT: &str = "output";
const TEMP: &str = "temp";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StopDetailsBus {
    time: String,
    line: String,
    destination: String,
    operating_days: String,
    school_restriction: String,
}

fn run_ocr(input: &Path, output: &Path) -> bool {
    let status = Command::new("ocrmypdf")
        .args([
            "--force-ocr",
            "--output-type",
            "pdfa",
            "--language",
            "pol",
            "--deskew",
            "--clean",
            "--clean-final",
        ])
        .arg(input)
        .arg(output)
        .status();
    status.map(|s| s.success()).unwrap_or(false)
}

fn suffix_parse(
    bus: &str,
    destination_map: &HashMap<String, String>,
    line_name: &str,
    line_number: &str,
    operating_days: &str,
) -> StopDetailsBus {
    let re = unsafe { Regex::new(r"(?P<time>\d{1,2}:\d{2})(?P<suffix>[A-Z]*)").unwrap_unchecked() };

    let caps = re.captures(bus.trim()).unwrap();

    let mut time = (&caps["time"]).to_string();
    let suffix = &caps["suffix"];

    let destination = match suffix.chars().next() {
        Some(suffix) => match destination_map.get(suffix.to_string().as_str()) {
            Some(destination) => destination.to_string(),
            None => line_name.to_string(),
        },
        None => line_name.to_string(),
    };

    let school_restriction = match suffix.chars().last() {
        Some('S') => "school_only",
        Some('W') => "free_day_only",
        _ => "normal",
    };

    if time.len() < 5 {
        time = format!("0{time}");
    }

    StopDetailsBus {
        time: time.to_string(),
        line: line_number.to_string(),
        destination: destination.to_string(),
        operating_days: operating_days.to_string(),
        school_restriction: school_restriction.to_string(),
    }
}

fn destination_update(original_des: &'_ str) -> &'_ str {
    let des = original_des.strip_suffix(". Nie kursuje").unwrap_or(original_des);
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

    let id = match id {
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
        "(hotel)" => "6000",
        _ => id,
    };

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
                result.insert(label, destination_update(v.get(1).unwrap().as_str()).to_string());
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
            .map(|time| suffix_parse(time, &destinations_map, destination, line_number, "mon_fri"))
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
                    "saturday",
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
            .map(|time| suffix_parse(time, &destinations_map, destination, line_number, "sunday"))
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

fn process_pdf(path: &PathBuf) -> HashMap<String, Vec<StopDetailsBus>> {
    let file_stem = path.file_stem().unwrap().to_string_lossy();
    let output_json = Path::new(OUTPUT).join(format!("{}.json", file_stem));

    let lines = extract_text(path);
    if let Ok(json_file) = File::create(&output_json) {
        serde_json::to_writer_pretty(json_file, &lines).unwrap();
        println!("Output file: {}", output_json.display());
    }
    lines
}

async fn download_pdf(pdf_link: String) {
    let pdf_name = pdf_link.split("/").last().unwrap();
    let output_path = Path::new(SOURCE).join(pdf_name);
    let pdf = reqwest::get(pdf_link).await.unwrap().bytes().await.unwrap();
    fs::write(output_path, pdf).unwrap();
}

fn check_pdf(pdf: &PathBuf) -> bool {
    const TOTAL: usize = 100;

    if let Ok(text) = pdf_extract::extract_text(&pdf) {
        let text = text.split_whitespace().collect::<Vec<&str>>().join(" ");
        let count = text
            .chars()
            .take(TOTAL)
            .filter(|c| {
                !c.is_control()
                    && (c.is_alphanumeric() || c.is_ascii_punctuation() || c.is_whitespace())
            })
            .count();
        count > (TOTAL as f32 * 0.8f32) as usize
    } else {
        false
    }
}

fn clear_dir(path: &str) -> std::io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    fs::create_dir_all(SOURCE).unwrap();
    fs::create_dir_all(OUTPUT).unwrap();
    fs::create_dir_all(TEMP).unwrap();

    let download = env::args().any(|arg| arg == "--download" || arg == "-d");
    let clear = env::args().any(|arg| arg == "--clear" || arg == "-c");
    let ignore_ocr = env::args().any(|arg| arg == "--ignore-ocr" || arg == "-i");

    if download {
        let download_start = Instant::now();
        println!("Downloading PDFs...");
        clear_dir(SOURCE).unwrap();
        let timetables = reqwest::get("https://zpgsa.bielawa.pl/rozklad-wazny-od-10-02-2025")
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let document = parse_html().one(timetables);

        let tasks = document.select("main p a").unwrap().map(|link| {
            let attributes = link.attributes.borrow();
            let pdf_link = attributes.get("href").unwrap().to_string();
            download_pdf(pdf_link)
        });

        join_all(tasks).await;
        println!("Download done in {:.2?}\n", download_start.elapsed());
    }

    if clear {
        let clear_start = Instant::now();
        println!("Clearing directories...");
        clear_dir(SOURCE).unwrap();
        clear_dir(OUTPUT).unwrap();
        clear_dir(TEMP).unwrap();
        println!("Clear done in {:.2?}\n", clear_start.elapsed());
    }

    let search_start = Instant::now();
    println!("Searching for PDFs...");
    let pdfs: Vec<PathBuf> = WalkDir::new(SOURCE)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_path_buf())
        .filter(|f| f.extension().map(|ext| ext == "pdf").unwrap_or(false))
        .collect();
    println!("Search done in {:.2?}\n", search_start.elapsed());

    let check_start = Instant::now();
    println!("Checking PDFs...\n");
    if ignore_ocr {
        println!("OCR will be ignored");
    }
    let pdfs: Vec<PathBuf> = pdfs
        .par_iter()
        .map(|pdf_path| {
            if !check_pdf(pdf_path) {
                println!("{} is corrupted", pdf_path.display());
                let fixed_path = Path::new(TEMP).join(pdf_path.file_name().unwrap());
                let txt_exist = if !fixed_path.exists() {
                    if !ignore_ocr {
                        run_ocr(pdf_path, &fixed_path)
                    } else {
                        false
                    }
                } else {
                    true
                };
                if txt_exist {
                    fs::write(
                        fixed_path.with_extension("txt"),
                        pdf_extract::extract_text(&fixed_path).unwrap(),
                    )
                    .unwrap();
                }
                fixed_path
            } else {
                pdf_path.to_path_buf()
            }
        })
        .collect();
    println!("Check done in {:.2?}\n", check_start.elapsed());

    println!("Parsing PDFs...\n");
    let parse_start = Instant::now();
    let result = pdfs.par_iter().map(|path| process_pdf(path)).reduce(
        || HashMap::new(),
        |mut acc, map| {
            acc.extend(map);
            acc
        },
    );
    println!("\nParse done in {:.2?}\n", parse_start.elapsed());

    let sorted: std::collections::BTreeMap<_, _> = result.into_iter().collect();

    if let Ok(json_file) = File::create("./output.json") {
        serde_json::to_writer_pretty(json_file, &sorted).unwrap();
    }
}
