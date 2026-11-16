//! Conservative identity for native-table custody. Only exact engine builds
//! qualify; interface executables may differ. This is not a Rust ABI standard.
use sha2::{Digest, Sha256};
use std::{env, fs, path::Path, process::Command};

fn sources(path: &Path, hash: &mut Sha256) -> std::io::Result<()> {
    let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            sources(&path, hash)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            hash.update(path.to_string_lossy().as_bytes());
            hash.update(fs::read(path)?);
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    if env::var_os("CARGO_FEATURE_SUCCESSION").is_none() {
        return Ok(());
    }
    let mut hash = Sha256::new();
    sources(Path::new("src"), &mut hash)?;
    for file in ["build.rs", "Cargo.toml", "../../Cargo.lock", "Cargo.lock"] {
        if let Ok(bytes) = fs::read(file) {
            hash.update(file.as_bytes());
            hash.update(bytes);
        }
    }
    let rustc = env::var("RUSTC")?;
    let compiler = Command::new(rustc)
        .args(["--version", "--verbose"])
        .output()?;
    if !compiler.status.success() {
        return Err("cannot identify Rust compiler".into());
    }
    hash.update(compiler.stdout);
    let mut vars: Vec<_> = env::vars()
        .filter(|(key, _)| {
            key.starts_with("CARGO_CFG_")
                || key.starts_with("CARGO_FEATURE_")
                || [
                    "CARGO_ENCODED_RUSTFLAGS",
                    "PROFILE",
                    "OPT_LEVEL",
                    "DEBUG",
                    "TARGET",
                    "HOST",
                    "RUSTC_WRAPPER",
                    "RUSTC_WORKSPACE_WRAPPER",
                ]
                .contains(&key.as_str())
        })
        .collect();
    vars.sort();
    for (key, value) in vars {
        println!("cargo:rerun-if-env-changed={key}");
        hash.update(key.as_bytes());
        hash.update([0]);
        hash.update(value.as_bytes());
        hash.update([0]);
    }
    let identity: [u8; 32] = hash.finalize().into();
    fs::write(
        Path::new(&env::var("OUT_DIR")?).join("succession_abi.rs"),
        format!("const ENGINE_ABI: [u8; 32] = {identity:?};\n"),
    )?;
    Ok(())
}
