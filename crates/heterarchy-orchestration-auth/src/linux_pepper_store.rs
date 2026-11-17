//! Linux private-file adapter for the local MCP bearer-token hash pepper.

use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path};

use rustix::fs::{
    AtFlags, Mode, OFlags, RenameFlags, fchmod, mkdirat, openat, renameat_with, unlinkat,
};
use rustix::io::Errno;
use rustix::process::geteuid;

use super::pepper_store::{LINUX_PEPPER_FILE_NAME, McpTokenPepper, McpTokenPepperError};

const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;
const MAX_TEMPORARY_CREATE_ATTEMPTS: usize = 8;

/// Store one hash pepper beneath the owner-only Orchestration application directory.
pub(super) struct LinuxMcpTokenPepperStore {
    directory: File,
}

impl LinuxMcpTokenPepperStore {
    /// Open or explicitly create the private application directory without following links.
    ///
    /// # Errors
    /// Rejects path aliases, unsafe ownership or permissions, and filesystem failures.
    pub(super) fn prepare(
        application_support: &Path,
        initialize: bool,
    ) -> Result<Self, McpTokenPepperError> {
        let directory = open_application_directory(application_support, initialize)?;
        let result = Self { directory };
        Ok(result)
    }

    /// Read the fixed-width pepper without following a final symlink.
    ///
    /// # Errors
    /// Rejects missing, linked, shared, wrongly owned, or malformed secret files.
    pub(super) fn load(&self) -> Result<Option<McpTokenPepper>, McpTokenPepperError> {
        let descriptor = match openat(
            &self.directory,
            LINUX_PEPPER_FILE_NAME,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error == Errno::NOENT => return Ok(None),
            Err(error) if error == Errno::LOOP => {
                return Err(McpTokenPepperError::LinuxFileSecurity);
            }
            Err(error) => return Err(linux_io(error)),
        };
        let mut file = File::from(descriptor);
        let metadata = file.metadata().map_err(linux_io)?;
        validate_secret_file(&metadata)?;
        if metadata.len() != 32 {
            return Err(McpTokenPepperError::CorruptLength {
                actual: usize::try_from(metadata.len()).unwrap_or(usize::MAX),
                expected: 32,
            });
        }
        let mut bytes = [0_u8; 32];
        file.read_exact(&mut bytes).map_err(linux_io)?;
        let mut trailing = [0_u8; 1];
        if file.read(&mut trailing).map_err(linux_io)? != 0 {
            return Err(McpTokenPepperError::CorruptLength {
                actual: 33,
                expected: 32,
            });
        }
        // A concurrent initializer may have just published this file. Make the
        // observed directory entry durable before its caller can create SQLite.
        self.directory.sync_all().map_err(linux_io)?;
        let result = Some(McpTokenPepper::new(bytes));
        Ok(result)
    }

    /// Atomically publish a caller-generated pepper without replacing an existing key.
    ///
    /// # Errors
    /// Returns `AlreadyExists` for a concurrent or prior creator and fails when durable
    /// publication or exact readback cannot be confirmed.
    pub(super) fn store_if_absent(
        &self,
        pepper: &McpTokenPepper,
    ) -> Result<(), McpTokenPepperError> {
        if self.load()?.is_some() {
            return Err(McpTokenPepperError::AlreadyExists);
        }

        let (temporary_name, mut temporary_file) = self.create_temporary_file()?;
        let write_result = (|| {
            temporary_file
                .write_all(pepper.as_bytes())
                .map_err(linux_io)?;
            temporary_file.sync_all().map_err(linux_io)
        })();
        drop(temporary_file);
        if let Err(error) = write_result {
            self.remove_temporary_file(&temporary_name);
            return Err(error);
        }

        match renameat_with(
            &self.directory,
            temporary_name.as_str(),
            &self.directory,
            LINUX_PEPPER_FILE_NAME,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => {}
            Err(error) if error == Errno::EXIST => {
                self.remove_temporary_file(&temporary_name);
                return Err(McpTokenPepperError::AlreadyExists);
            }
            Err(error) => {
                self.remove_temporary_file(&temporary_name);
                return Err(linux_io(error));
            }
        }
        self.directory.sync_all().map_err(linux_io)?;

        match self.load()? {
            Some(stored) if stored == *pepper => Ok(()),
            Some(_) => Err(McpTokenPepperError::ConcurrentReplacement),
            None => Err(McpTokenPepperError::WriteUnconfirmed),
        }
    }

    fn create_temporary_file(&self) -> Result<(String, File), McpTokenPepperError> {
        for _attempt in 0..MAX_TEMPORARY_CREATE_ATTEMPTS {
            let mut random = [0_u8; 16];
            getrandom::fill(&mut random).map_err(McpTokenPepperError::LinuxRandomness)?;
            let temporary_name = format!(
                ".{LINUX_PEPPER_FILE_NAME}.{:032x}.tmp",
                u128::from_be_bytes(random)
            );
            let descriptor = match openat(
                &self.directory,
                temporary_name.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::from_raw_mode(PRIVATE_FILE_MODE),
            ) {
                Ok(descriptor) => descriptor,
                Err(error) if error == Errno::EXIST => continue,
                Err(error) => return Err(linux_io(error)),
            };
            let file = File::from(descriptor);
            if let Err(error) = fchmod(&file, Mode::from_raw_mode(PRIVATE_FILE_MODE)) {
                drop(file);
                self.remove_temporary_file(&temporary_name);
                return Err(linux_io(error));
            }
            let metadata = match file.metadata() {
                Ok(metadata) => metadata,
                Err(error) => {
                    drop(file);
                    self.remove_temporary_file(&temporary_name);
                    return Err(linux_io(error));
                }
            };
            if let Err(error) = validate_secret_file(&metadata) {
                drop(file);
                self.remove_temporary_file(&temporary_name);
                return Err(error);
            }
            let result = (temporary_name, file);
            return Ok(result);
        }
        Err(McpTokenPepperError::LinuxFileSecurity)
    }

    fn remove_temporary_file(&self, name: &str) {
        let _result = unlinkat(&self.directory, name, AtFlags::empty());
    }
}

fn open_application_directory(path: &Path, initialize: bool) -> Result<File, McpTokenPepperError> {
    if !path.is_absolute() {
        return Err(McpTokenPepperError::LinuxPathSecurity);
    }
    let components = path
        .components()
        .filter_map(|component| match component {
            Component::RootDir => Some(Ok(None)),
            Component::Normal(value) => Some(Ok(Some(value.to_owned()))),
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                Some(Err(McpTokenPepperError::LinuxPathSecurity))
            }
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let Some(last_index) = components.len().checked_sub(1) else {
        return Err(McpTokenPepperError::LinuxPathSecurity);
    };

    let mut current = File::open("/").map_err(linux_io)?;
    for (index, component) in components.iter().enumerate() {
        let is_application_directory = index == last_index;
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
        let next = match openat(&current, component, flags, Mode::empty()) {
            Ok(descriptor) => File::from(descriptor),
            Err(error) if error == Errno::NOENT && initialize => {
                let created = match mkdirat(
                    &current,
                    component,
                    Mode::from_raw_mode(PRIVATE_DIRECTORY_MODE),
                ) {
                    Ok(()) => true,
                    Err(error) if error == Errno::EXIST => false,
                    Err(error) => return Err(linux_io(error)),
                };
                let descriptor =
                    openat(&current, component, flags, Mode::empty()).map_err(linux_io)?;
                let directory = File::from(descriptor);
                if created {
                    let metadata = directory.metadata().map_err(linux_io)?;
                    if !metadata.is_dir() || metadata.uid() != geteuid().as_raw() {
                        return Err(McpTokenPepperError::LinuxPathSecurity);
                    }
                    fchmod(&directory, Mode::from_raw_mode(PRIVATE_DIRECTORY_MODE))
                        .map_err(linux_io)?;
                    directory.sync_all().map_err(linux_io)?;
                    current.sync_all().map_err(linux_io)?;
                }
                directory
            }
            Err(error) if error == Errno::LOOP => {
                return Err(McpTokenPepperError::LinuxPathSecurity);
            }
            Err(error) => return Err(linux_io(error)),
        };
        validate_directory(&next, is_application_directory)?;
        current = next;
    }
    let result = current;
    Ok(result)
}

fn validate_directory(
    directory: &File,
    application_directory: bool,
) -> Result<(), McpTokenPepperError> {
    let metadata = directory.metadata().map_err(linux_io)?;
    if !metadata.is_dir() {
        return Err(McpTokenPepperError::LinuxPathSecurity);
    }
    let owner = metadata.uid();
    let mode = metadata.mode() & 0o7777;
    let effective_uid = geteuid().as_raw();
    if application_directory {
        if owner != effective_uid || mode != PRIVATE_DIRECTORY_MODE {
            return Err(McpTokenPepperError::LinuxPathSecurity);
        }
    } else if owner != effective_uid && owner != 0 {
        return Err(McpTokenPepperError::LinuxPathSecurity);
    } else if mode & 0o022 != 0 && (owner != 0 || mode & 0o1000 == 0) {
        return Err(McpTokenPepperError::LinuxPathSecurity);
    }
    Ok(())
}

fn validate_secret_file(metadata: &std::fs::Metadata) -> Result<(), McpTokenPepperError> {
    if !metadata.is_file()
        || metadata.uid() != geteuid().as_raw()
        || metadata.mode() & 0o7777 != PRIVATE_FILE_MODE
        || metadata.nlink() != 1
    {
        return Err(McpTokenPepperError::LinuxFileSecurity);
    }
    Ok(())
}

fn linux_io(error: impl Into<std::io::Error>) -> McpTokenPepperError {
    McpTokenPepperError::LinuxIo(error.into())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use rustix::fs::mkfifoat;

    use super::*;

    fn temporary_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!("orch-token-pepper-{nonce}"));
        fs::create_dir(&path)?;
        let result = path;
        Ok(result)
    }

    #[test]
    fn private_pepper_is_fixed_width_persistent_and_0600() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = temporary_root()?;
        let application_support = root.join("support").join("Heterarchy Forge");
        let store = LinuxMcpTokenPepperStore::prepare(&application_support, true)?;
        let expected = McpTokenPepper::new([0x2a; 32]);

        assert_eq!(store.load()?, None);
        store.store_if_absent(&expected)?;
        assert_eq!(store.load()?, Some(expected.clone()));
        assert_eq!(fs::metadata(&application_support)?.mode() & 0o7777, 0o700);
        let secret_path = application_support.join(LINUX_PEPPER_FILE_NAME);
        let metadata = fs::metadata(&secret_path)?;
        assert_eq!(metadata.mode() & 0o7777, 0o600);
        assert_eq!(metadata.uid(), geteuid().as_raw());
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(metadata.len(), 32);

        let reopened = LinuxMcpTokenPepperStore::prepare(&application_support, false)?;
        assert_eq!(reopened.load()?, Some(expected));
        assert!(matches!(
            reopened.store_if_absent(&McpTokenPepper::new([0x4b; 32])),
            Err(McpTokenPepperError::AlreadyExists)
        ));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn creation_competition_never_replaces_the_published_pepper()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = temporary_root()?;
        let application_support = root.join("Heterarchy Forge");
        let first = LinuxMcpTokenPepperStore::prepare(&application_support, true)?;
        let second = LinuxMcpTokenPepperStore::prepare(&application_support, false)?;
        let first_pepper = McpTokenPepper::new([0x11; 32]);
        let second_pepper = McpTokenPepper::new([0x22; 32]);

        let first_thread = thread::spawn(move || first.store_if_absent(&first_pepper));
        let second_thread = thread::spawn(move || second.store_if_absent(&second_pepper));
        let outcomes = [
            first_thread.join().map_err(|_| "first creator panicked")?,
            second_thread
                .join()
                .map_err(|_| "second creator panicked")?,
        ];
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, Err(McpTokenPepperError::AlreadyExists)))
                .count(),
            1
        );
        let reader = LinuxMcpTokenPepperStore::prepare(&application_support, false)?;
        assert!(reader.load()?.is_some());

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn directory_alias_and_insecure_existing_directory_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = temporary_root()?;
        let target = root.join("private");
        fs::create_dir(&target)?;
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755))?;
        assert!(matches!(
            LinuxMcpTokenPepperStore::prepare(&target, false),
            Err(McpTokenPepperError::LinuxPathSecurity)
        ));

        let link = root.join("linked");
        symlink(&target, &link)?;
        let linked_application_support = link.join("nested").join("Heterarchy Forge");
        assert!(matches!(
            LinuxMcpTokenPepperStore::prepare(&linked_application_support, true),
            Err(McpTokenPepperError::LinuxPathSecurity) | Err(McpTokenPepperError::LinuxIo(_))
        ));
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn linked_hardlinked_and_shared_pepper_files_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = temporary_root()?;
        let application_support = root.join("Heterarchy Forge");
        let store = LinuxMcpTokenPepperStore::prepare(&application_support, true)?;
        let secret_path = application_support.join(LINUX_PEPPER_FILE_NAME);
        fs::write(&secret_path, [0x31; 32])?;
        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o600))?;
        let hard_link = root.join("hardlink");
        fs::hard_link(&secret_path, &hard_link)?;
        assert!(matches!(
            store.load(),
            Err(McpTokenPepperError::LinuxFileSecurity)
        ));
        fs::remove_file(&secret_path)?;
        fs::remove_file(&hard_link)?;

        fs::write(&secret_path, [0x32; 32])?;
        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o640))?;
        assert!(matches!(
            store.load(),
            Err(McpTokenPepperError::LinuxFileSecurity)
        ));
        fs::remove_file(&secret_path)?;

        let linked_target = root.join("linked-target");
        fs::write(&linked_target, [0x33; 32])?;
        fs::set_permissions(&linked_target, fs::Permissions::from_mode(0o600))?;
        symlink(&linked_target, &secret_path)?;
        assert!(matches!(
            store.load(),
            Err(McpTokenPepperError::LinuxFileSecurity) | Err(McpTokenPepperError::LinuxIo(_))
        ));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn fifo_pepper_path_is_rejected_without_blocking() -> Result<(), Box<dyn std::error::Error>> {
        const CHILD_DIRECTORY_ENV: &str = "HETERARCHY_ORCHESTRATION_FIFO_PEPPER_TEST_DIRECTORY";
        if let Some(directory) = std::env::var_os(CHILD_DIRECTORY_ENV) {
            let store = LinuxMcpTokenPepperStore::prepare(Path::new(&directory), false)?;
            assert!(matches!(
                store.load(),
                Err(McpTokenPepperError::LinuxFileSecurity)
            ));
            return Ok(());
        }

        let root = temporary_root()?;
        let application_support = root.join("Heterarchy Forge");
        let store = LinuxMcpTokenPepperStore::prepare(&application_support, true)?;
        mkfifoat(
            &store.directory,
            LINUX_PEPPER_FILE_NAME,
            Mode::from_raw_mode(PRIVATE_FILE_MODE),
        )?;
        let test_binary = std::env::current_exe()?;
        let mut child = Command::new(test_binary)
            .args([
                "--exact",
                "linux_pepper_store::tests::fifo_pepper_path_is_rejected_without_blocking",
            ])
            .env(CHILD_DIRECTORY_ENV, application_support.as_os_str())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break Ok(status);
            }
            if Instant::now() >= deadline {
                child.kill()?;
                break Err(child.wait_with_output()?);
            }
            thread::sleep(Duration::from_millis(10));
        };
        drop(store);
        fs::remove_dir_all(root)?;
        match status {
            Ok(status) => assert!(status.success(), "FIFO reader child failed: {status}"),
            Err(output) => {
                return Err(format!(
                    "FIFO open exceeded the deadline and child was terminated: {}",
                    String::from_utf8_lossy(&output.stderr)
                )
                .into());
            }
        }
        Ok(())
    }
}
