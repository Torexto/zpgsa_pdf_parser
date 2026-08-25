use anyhow::{Result, anyhow};

use std::path::PathBuf;

pub fn check_pdf(pdf_path: PathBuf) -> Result<PathBuf> {
    const TOTAL: usize = 100;

    let text = pdf_extract::extract_text(&pdf_path)?;

    let text = text.split_whitespace().collect::<Vec<&str>>().join(" ");

    let count = text
        .chars()
        .take(TOTAL)
        .filter(|c| {
            !c.is_control()
                && (c.is_alphanumeric() || c.is_ascii_punctuation() || c.is_whitespace())
        })
        .count();

    let correct = count > (TOTAL as f32 * 0.8f32) as usize;

    if !correct {
        println!("PDF is corrupted: {}", pdf_path.display());
        return Err(anyhow!("PDF is corrupted"));
    }

    Ok(pdf_path)
}
