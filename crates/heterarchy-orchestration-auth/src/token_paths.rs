//! Token authority paths owned by Orchestration, retaining installed identities.

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenAuthorityPaths {
    application_support: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TokenPathError {
    #[error("token authority home directory is unavailable or invalid")]
    InvalidHome,
    #[error("token authority paths are unsupported on this platform")]
    UnsupportedPlatform,
}

impl TokenAuthorityPaths {
    /// Discover the historical per-user credential directory without creating it.
    ///
    /// # Errors
    /// Rejects missing, relative or unavailable home directories and unsupported hosts.
    pub fn discover() -> Result<Self, TokenPathError> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or(TokenPathError::InvalidHome)?;
        Self::from_home(&home)
    }

    fn from_home(home: &Path) -> Result<Self, TokenPathError> {
        if !home.is_absolute()
            || !home.is_dir()
            || home
                .canonicalize()
                .map_err(|_| TokenPathError::InvalidHome)?
                != home
        {
            return Err(TokenPathError::InvalidHome);
        }
        #[cfg(target_os = "macos")]
        let base = home.join("Library").join("Application Support");
        #[cfg(all(unix, not(target_os = "macos")))]
        let base = match std::env::var_os("XDG_DATA_HOME").map(PathBuf::from) {
            Some(path) if path.is_absolute() => path,
            _ => home.join(".local").join("share"),
        };
        #[cfg(not(unix))]
        return Err(TokenPathError::UnsupportedPlatform);
        #[cfg(unix)]
        Ok(Self {
            application_support: base.join("Heterarchy Forge"),
        })
    }

    /// Preserved directory containing the existing token catalog.
    #[must_use]
    pub fn application_support(&self) -> &Path {
        &self.application_support
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn discovery_preserves_installed_identity_without_creating_catalog_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = std::env::temp_dir().join(format!("token-home-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&home)?;
        let canonical_home = home.canonicalize()?;
        let paths = TokenAuthorityPaths::from_home(&canonical_home)?;
        assert_eq!(
            paths.application_support(),
            canonical_home.join("Library/Application Support/Heterarchy Forge")
        );
        assert!(!paths.application_support().exists());
        assert!(TokenAuthorityPaths::from_home(Path::new("relative-home")).is_err());
        let alias = home.with_extension("alias");
        std::os::unix::fs::symlink(&canonical_home, &alias)?;
        assert!(TokenAuthorityPaths::from_home(&alias).is_err());
        std::fs::remove_file(alias)?;
        std::fs::remove_dir_all(home)?;
        Ok(())
    }
}
