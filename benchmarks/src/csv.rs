use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

pub struct CsvWriter {
    path: Option<std::path::PathBuf>,
    header_written: bool,
    header: Vec<&'static str>,
}

impl CsvWriter {
    pub fn new<P: AsRef<Path>>(path: Option<P>, header: Vec<&'static str>) -> Self {
        Self {
            path: path.map(|p| p.as_ref().to_path_buf()),
            header_written: false,
            header,
        }
    }

    pub fn write_row(&mut self, row: &[String]) -> std::io::Result<()> {
        let Some(path) = self.path.clone() else {
            return Ok(());
        };
        if !self.header_written {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let exists = path.exists();
            let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
            if !exists {
                writeln!(f, "{}", self.header.join(","))?;
            }
            self.header_written = true;
            writeln!(f, "{}", row.join(","))?;
            return Ok(());
        }
        let mut f = OpenOptions::new().append(true).open(&path)?;
        writeln!(f, "{}", row.join(","))?;
        Ok(())
    }
}
