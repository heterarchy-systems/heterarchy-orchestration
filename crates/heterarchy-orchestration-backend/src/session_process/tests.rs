use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use super::*;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("session-host-{}", uuid::Uuid::now_v7()));
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        Ok(Self(root.canonicalize()?))
    }

    fn secret(&self) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = self.0.join("issuer-secret");
        fs::write(&path, "isolated-test-issuer\n")?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        Ok(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn issuer_secret_rejects_shared_permissions_and_symlink_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new()?;
    let secret = fixture.secret()?;
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o640))?;
    assert!(read_secret(&secret).is_err());
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600))?;
    let alias = fixture.0.join("alias");
    std::os::unix::fs::symlink(&secret, &alias)?;
    assert!(read_secret(&alias).is_err());
    assert_eq!(read_secret(&secret)?, "isolated-test-issuer");
    Ok(())
}

#[test]
fn command_requires_exact_explicit_fields_and_absolute_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let arguments = |values: &[&str]| values.iter().map(OsString::from).collect::<Vec<_>>();
    assert!(parse(&arguments(&["--help"]))?.is_none());
    let valid = arguments(&[
        "session-authority",
        "--port",
        "0",
        "--catalog",
        "/catalog",
        "--issuer-secret-file",
        "/secret",
    ]);
    let options = parse(&valid)?.ok_or("missing command")?;
    assert_eq!(options.port, 0);
    assert_eq!(options.catalog_path, PathBuf::from("/catalog"));
    for values in [
        vec!["session-authority"],
        vec![
            "session-authority",
            "--port",
            "65536",
            "--catalog",
            "/catalog",
            "--issuer-secret-file",
            "/secret",
        ],
        vec![
            "session-authority",
            "--port",
            "0",
            "--catalog",
            "relative",
            "--issuer-secret-file",
            "/secret",
        ],
        vec![
            "session-authority",
            "--port",
            "0",
            "--catalog",
            "/catalog",
            "--issuer-secret-file",
            "/secret",
            "--port",
            "1",
        ],
        vec![
            "session-authority",
            "--port",
            "0",
            "--catalog",
            "/catalog",
            "--issuer-secret-file",
            "/secret",
            "extra",
        ],
    ] {
        assert!(parse(&arguments(&values)).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn bootstrap_never_creates_a_missing_catalog() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new()?;
    let secret_path = fixture.secret()?;
    let catalog_path = fixture.0.join("missing.sqlite3");
    let result = run_session_authority_process(SessionAuthorityOptions {
        port: 0,
        catalog_path: catalog_path.clone(),
        secret_path,
    })
    .await;
    assert!(matches!(
        result,
        Err(SessionAuthorityProcessError::CatalogMissing)
    ));
    assert!(!catalog_path.exists());
    Ok(())
}
