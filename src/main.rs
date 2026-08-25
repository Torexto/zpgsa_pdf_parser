use std::env;
use anyhow::Result;
use std::time::Instant;

use zpgsa_pdf_parser::{check_pdfs, clear_dirs, download_pdfs, process_pdfs, scan_pdfs, save_output};

#[tokio::main]
async fn main() -> Result<()> {
    clear_dirs().await?;

    let download = env::args().any(|arg| arg == "--download" || arg == "-d");

    let program_timer = Instant::now();

    if download {
        let download_timer = Instant::now();
        println!("Downloading PDFs...");

        download_pdfs().await?;

        println!("Download done in {:.2?}\n", download_timer.elapsed());
    }

    let search_timer = Instant::now();
    println!("Searching for PDFs...");

    let pdfs = scan_pdfs().await?;

    println!("Search done in {:.2?}\n", search_timer.elapsed());

    let check_start = Instant::now();
    println!("Checking PDFs...\n");

    let correct_pdfs = check_pdfs(pdfs).await?;

    println!("Check done in {:.2?}\n", check_start.elapsed());

    let process_timer = Instant::now();
    println!("Processing PDFs...\n");

    let result = tokio::task::spawn_blocking(move || process_pdfs(correct_pdfs)).await?;

    println!("\nParse done in {:.2?}\n", process_timer.elapsed());

    save_output("./output.json", &result)?;

    println!("Total time: {:.2?}", program_timer.elapsed());

    Ok(())
}
