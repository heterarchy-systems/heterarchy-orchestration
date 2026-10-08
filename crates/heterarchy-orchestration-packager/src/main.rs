//! Tracked Orchestration headless Backend packager.

mod package;
mod process;

use std::env;
use std::path::{Path, PathBuf};

fn main() {
    if let Err(error) = run() {
        eprintln!("heterarchy-orchestration-packager: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let root = workspace_root()?;
    let mut arguments = env::args().skip(1);
    let command = arguments.next();
    if let Some(extra) = arguments.next() {
        return Err(format!("unexpected argument {extra:?}"));
    }
    match command.as_deref().unwrap_or("help") {
        "package" => package::run(&root),
        "help" | "--help" | "-h" => {
            println!("usage: heterarchy-orchestration-packager <package|help>");
            Ok(())
        }
        other => Err(format!("unknown command {other:?}")),
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            format!(
                "packager manifest directory has no repository root: {}",
                manifest_dir.display()
            )
        })
}
