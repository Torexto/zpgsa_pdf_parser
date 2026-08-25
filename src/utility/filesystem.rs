use anyhow::{Context, Result};
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

pub async fn clear_dir(path: &str) -> Result<()> {
    for entry in walkdir::WalkDir::new(path) {
        match entry {
            Ok(entry) => {
                let path = entry.path();
                if path.is_file() {
                    tokio::fs::remove_file(path).await?;
                }
            }
            Err(_) => {}
        }
    }
    Ok(())
}

pub fn save_output<P, T>(path: P, data: &T) -> Result<()>
where
    P: AsRef<Path>,
    T: Serialize,
{
    let path_ref = path.as_ref();
    let file = File::create(path_ref)
        .with_context(|| format!("Nie udało się utworzyć pliku {:?}", path_ref))?;

    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, data)?;

    println!("Output file: {:?}\n", path_ref);
    Ok(())
}
