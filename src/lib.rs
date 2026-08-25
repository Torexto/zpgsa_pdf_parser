use crate::parser::check::check_pdf;
use crate::parser::parser::process_pdf;
use anyhow::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use utility::filesystem::clear_dir;
use utility::html::{download_pdf, get_pdf_list};
use walkdir::WalkDir;

mod parser;
mod utility;

pub use utility::filesystem::save_output;

const SOURCE: &str = "source";
const OUTPUT: &str = "output";
const TEMP: &str = "temp";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StopDetailsBus {
    pub time: String,
    pub line: String,
    pub destination: String,
    pub operating_days: String,
    pub school_restriction: String,
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

pub fn process_pdfs(pdfs: Vec<PathBuf>) -> HashMap<String, Vec<StopDetailsBus>> {
    let result: HashMap<String, Vec<StopDetailsBus>> =
        pdfs.into_par_iter().flat_map(process_pdf).collect();

    result
}
