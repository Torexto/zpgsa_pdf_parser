use rayon::prelude::*;
use std::io::Read;
use std::{
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
    process::Command,
};
use walkdir::WalkDir;

const INPUT_DIR: &str = "pdfs";
const OUTPUT_DIR: &str = "json";
const OCR_TEMP: &str = "ocr_temp";

fn run_ocr(input: &Path, output: &Path) -> bool {
    let status = Command::new("ocrmypdf")
        .args(["--force-ocr", "--output-type", "pdf", "--language", "pol"])
        .arg(input)
        .arg(output)
        .status();
    status.map(|s| s.success()).unwrap_or(false)
}

fn extract_text(pdf: &Path) -> Vec<String> {
    let mut pages = Vec::new();
    let tmp_txt = pdf.with_extension("txt");

    let _ = Command::new("pdftotext")
        .arg("-layout")
        .arg(pdf)
        .arg(&tmp_txt)
        .status();

    if let Ok(file) = File::open(&tmp_txt) {
        let mut reader = BufReader::new(file);
        let mut text = String::new();

        if let Ok(_) = reader.read_to_string(&mut text) {
            let text = text.split_whitespace().map(|s| format!("{} ", s)).collect::<String>();
            let mut lines = text.split("LINIA: Rozkład jazdy ważny od: 17.03.2025 r.");
            for line in lines {
                println!("{}", line);
                println!("------------------------------");
                pages.push(line.to_string());
            }
        }
    }

    // let _ = fs::remove_file(&tmp_txt);
    pages
}

fn process_pdf(path: &PathBuf) {
    let file_stem = path.file_stem().unwrap().to_string_lossy();
    let output_json = Path::new(OUTPUT_DIR).join(format!("{}.json", file_stem));
    let ocr_fixed = Path::new(OCR_TEMP).join(path.file_name().unwrap());

    let needs_ocr = false;

    let parse_target = if needs_ocr {
        if run_ocr(&path, &ocr_fixed) {
            ocr_fixed
        } else {
            eprintln!("Failed OCR: {}", path.display());
            return;
        }
    } else {
        ocr_fixed
    };

    let pages = extract_text(&parse_target);
    if let Ok(json_file) = File::create(output_json) {
        serde_json::to_writer_pretty(json_file, &pages).unwrap();
    }
}

fn main() {
    fs::create_dir_all(OUTPUT_DIR).unwrap();
    fs::create_dir_all(OCR_TEMP).unwrap();

    let pdfs: Vec<_> = WalkDir::new(INPUT_DIR)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_path_buf())
        .filter(|f| f.extension().map(|ext| ext == "pdf").unwrap_or(false))
        .collect();

    pdfs.par_iter().for_each(|path| process_pdf(path));
}
