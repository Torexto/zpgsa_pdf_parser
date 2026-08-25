use anyhow::{Context, Result};
use kuchiki::traits::TendrilSink;

const TIMETABLES_URL: &str = "https://zpgsa.bielawa.pl/rozklad-wazny-od-10-02-2025";

pub async fn get_pdf_list() -> Result<Vec<String>> {
    let response = reqwest::get(TIMETABLES_URL).await?;
    let document_content = response.text().await?;

    get_pdf_urls(&document_content).await
}

async fn get_pdf_urls(content: &str) -> Result<Vec<String>> {
    let document = kuchiki::parse_html().one(content);

    let matching_links = document
        .select("main p a")
        .map_err(|_| anyhow::anyhow!("No links found"))?;

    let pdf_urls: Vec<String> = matching_links
        .filter_map(|link| {
            let attributes = link.attributes.borrow();
            let href = attributes.get("href")?;

            if href.ends_with(".pdf") {
                Some(href.to_string())
            } else {
                None
            }
        })
        .collect();

    Ok(pdf_urls)
}

pub async fn download_pdf(pdf_link: &str) -> Result<()> {
    let pdf_name = pdf_link
        .split('/')
        .last()
        .context("Nieprawidłowy URL pliku PDF")?;

    let output_path = std::path::Path::new("source").join(pdf_name);

    let response = reqwest::get(pdf_link).await?;
    let pdf_bytes = response.bytes().await?;

    tokio::fs::write(output_path, pdf_bytes).await?;

    Ok(())
}
