use anyhow::Result;
use std::env;
use std::time::Instant;

use zpgsa_pdf_parser::{check_pdfs, clear_dirs, count_stops, download_pdfs, load_backup, load_expected_counts, process_pdfs, repair_from_backup, save_output, scan_pdfs, verify_counts, Mismatch, RepairReport};

const COUNTS: &str = "./count.json";
const BACKUP: &str = "./backup.json";
const OUTPUT: &str = "./output.json";

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

    let mut result = tokio::task::spawn_blocking(move || process_pdfs(correct_pdfs)).await?;

    println!("\nParse done in {:.2?}\n", process_timer.elapsed());

    let check_timer = Instant::now();
    println!("Checking results...\n");

    match load_expected_counts(COUNTS) {
        Ok(expected_counts) => {
            let mut mismatches = verify_counts(&count_stops(&result), &expected_counts);

            if !mismatches.is_empty() {
                println!(
                    "❌ Znaleziono {} rozbieżności – próba naprawy z backupu {BACKUP}...\n",
                    mismatches.len()
                );

                match load_backup(BACKUP) {
                    Ok(backup) => {
                        let report = repair_from_backup(&mut result, &backup, &mismatches);
                        print_repair_report(&report);

                        // Ponowna weryfikacja – naprawiony wynik musi zgadzać się ze wzorcem.
                        mismatches = verify_counts(&count_stops(&result), &expected_counts);
                    }
                    Err(e) => {
                        eprintln!("Nie udało się załadować backupu: {e:#}\n");
                    }
                }
            }

            print_verification_report(&mismatches);
            
            if !mismatches.is_empty() {
                save_output(BACKUP, &result)?;
            }
        }
        Err(e) => {
            eprintln!("Błąd podczas ładowania pliku wzorcowego: {e}");
        }
    }

    println!("Check done in {:.2?}\n", check_timer.elapsed());

    save_output(OUTPUT, &result)?;

    println!("Total time: {:.2?}", program_timer.elapsed());

    Ok(())
}

pub fn print_verification_report(mismatches: &[Mismatch]) {
    if mismatches.is_empty() {
        println!("✅ Wszystkie liczby odjazdów zgadzają się ze wzorcem!");
        return;
    }

    println!("❌ Pozostało {} rozbieżności:\n", mismatches.len());
    println!("{:<20} | {:<8} | {:<20} | {:<10} | {:<10}", "Przystanek", "Linia", "Dni", "Oczekiwano", "Pobrano");
    println!("{}", "-".repeat(75));

    for m in mismatches {
        println!(
            "{:<20} | {:<8} | {:<20?} | {:<10} | {:<10}",
            m.stop_id, m.line, m.day, m.expected, m.actual
        );
    }

    println!("\n⚠️  Backup nie potwierdza powyższych grup – dane w wyniku są niepoprawne.");
}

pub fn print_repair_report(report: &RepairReport) {
    println!(
        "🔧 Naprawiono z backupu: {} grup, bez pokrycia w backupie: {} grup\n",
        report.repaired.len(),
        report.unresolved.len()
    );

    if report.repaired.is_empty() {
        return;
    }

    println!("{:<20} | {:<8} | {:<20} | {:<10} | {:<10}", "Przystanek", "Linia", "Dni", "Pobrano", "Z backupu");
    println!("{}", "-".repeat(75));

    for r in &report.repaired {
        println!(
            "{:<20} | {:<8} | {:<20?} | {:<10} | {:<10}",
            r.stop_id, r.line, r.day, r.parsed, r.restored
        );
    }

    println!();
}