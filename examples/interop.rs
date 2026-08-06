//! Write a `.fsdb` in Rust, then open it from Python — no conversion step.
//!
//!     cargo run --example interop
//!
//! Requires the Python package (`pip install fusedb`) for the second half.

use fusedb::{FuseReader, FuseWriter};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Org {
    company: String,
    asn: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("fusedb-interop.fsdb");

    let mut w = FuseWriter::new();
    w.add(
        "cloudflare.com",
        &Org {
            company: "Cloudflare".into(),
            asn: 13335,
        },
    )?;
    w.build(&path)?;
    println!("rust wrote {}", path.display());

    // Same file, read back through the Rust API…
    let db = FuseReader::open(&path)?;
    println!("rust reads  {:?}", db.get::<Org>("cloudflare.com")?);

    // …and through Python, with no conversion in between.
    let script = format!(
        r#"
from fusedb import FuseReader
with FuseReader({:?}) as db:
    print("python reads", db.get("cloudflare.com"))
"#,
        path.to_string_lossy(),
    );

    match std::process::Command::new("python3")
        .args(["-c", &script])
        .status()
    {
        Ok(status) if status.success() => {}
        Ok(_) => eprintln!("python3 could not read the file — is `fusedb` installed?"),
        Err(e) => eprintln!("skipping the Python half: {e}"),
    }

    std::fs::remove_file(&path).ok();
    Ok(())
}
