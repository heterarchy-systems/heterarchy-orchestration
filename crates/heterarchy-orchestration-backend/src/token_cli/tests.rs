use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::PathBuf;

use heterarchy_orchestration_auth::{
    MCP_TOKEN_DIGEST_BYTES, McpTokenCatalogPort, McpTokenDigest, McpTokenId, McpTokenIssueRequest,
    McpTokenIssued, McpTokenLabel, McpTokenMetadata, McpTokenRevision, McpTokenService,
    RegisteredMcpToken,
};
use uuid::Uuid;

use super::{
    SecretOutput, TokenAuthorityFailure, TokenCliAuthority, TokenCliCommand, TokenCliError,
    TokenIssueOptions, issue_reserved, issue_with_authority, parse,
};

#[test]
fn parser_requires_explicit_bootstrap_and_complete_output_contract() -> Result<(), Box<dyn Error>> {
    let command = parse(&arguments(&[
        "token",
        "issue",
        "--initialize-authority",
        "--label",
        "local operator",
        "--output",
        "/tmp/orchestration-token",
    ]))?;
    let TokenCliCommand::Issue(options) = command else {
        return Err("expected token issue command".into());
    };
    assert_eq!(options.label(), "local operator");
    assert_eq!(
        options.output_path(),
        PathBuf::from("/tmp/orchestration-token")
    );

    assert!(matches!(
        parse(&arguments(&[
            "token",
            "issue",
            "--label",
            "local operator",
            "--output",
            "/tmp/orchestration-token",
        ])),
        Err(TokenCliError::InitializationNotExplicit)
    ));
    assert!(matches!(
        parse(&arguments(&[
            "token",
            "issue",
            "--initialize-authority",
            "--label",
            "bad\nlabel",
            "--output",
            "/tmp/orchestration-token",
        ])),
        Err(TokenCliError::InvalidLabel)
    ));
    Ok(())
}

#[test]
fn issue_writes_only_to_a_new_mode_0600_file_and_returns_metadata() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;

    let path = TestPath::new("private-output");
    let mut service = standalone_service();
    let metadata = issue_with_authority(&options("local operator", path.path())?, &mut service)?;
    let reveal = fs::read_to_string(path.path())?;
    let reveal = reveal.trim_end_matches('\n');

    assert_eq!(reveal.len(), 43);
    assert_eq!(
        fs::metadata(path.path())?.permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        service
            .authenticate(&format!("Bearer {reveal}"))?
            .map(|authorization| authorization.token_id()),
        Some(metadata.token_id())
    );
    assert_eq!(metadata.label(), "local operator");
    assert!(!format!("{metadata:?}").contains(reveal));
    Ok(())
}

#[test]
fn issue_refuses_existing_output_before_touching_token_authority() -> Result<(), Box<dyn Error>> {
    let path = TestPath::new("existing-output");
    fs::write(path.path(), b"keep existing bytes")?;
    let mut service = standalone_service();

    assert!(matches!(
        issue_with_authority(&options("local operator", path.path())?, &mut service),
        Err(TokenCliError::OutputAlreadyExists)
    ));
    assert_eq!(fs::read(path.path())?, b"keep existing bytes");
    assert!(service.list(256)?.is_empty());
    Ok(())
}

#[test]
fn unknown_issue_is_read_back_once_and_never_blindly_retried() -> Result<(), Box<dyn Error>> {
    let path = TestPath::new("unknown-output");
    let mut authority = UnknownAfterCommit {
        inner: standalone_service(),
        issue_calls: 0,
    };
    let result = issue_with_authority(&options("uncertain operator", path.path())?, &mut authority);

    let Err(TokenCliError::IssueOutcomeUnknown {
        possible_token_ids,
        readback_succeeded,
    }) = result
    else {
        return Err("expected an unknown outcome with durable readback identity".into());
    };
    let records = authority.inner.list(256)?;
    assert_eq!(authority.issue_calls, 1);
    assert!(readback_succeeded);
    assert_eq!(records.len(), 1);
    assert_eq!(possible_token_ids, vec![records[0].id().to_string()]);
    assert_eq!(records[0].label().as_str(), "uncertain operator");
    assert_eq!(fs::read(path.path())?, vec![0_u8; 44]);
    Ok(())
}

#[test]
fn committed_token_with_failed_secret_write_reports_exact_id_without_reveal()
-> Result<(), Box<dyn Error>> {
    let mut service = standalone_service();
    let mut output = FailingOutput {
        path: PathBuf::from("test-output"),
        observed_reveal: String::new(),
    };
    let label = McpTokenLabel::new("failed output operator".to_owned())?;
    let result = issue_reserved(&mut service, &mut output, &label);

    let Err(error) = result else {
        return Err("expected secure-output failure after token issue".into());
    };
    let TokenCliError::SecretOutputUnconfirmed { token_id, .. } = &error else {
        return Err("expected the exact issued token identity in the failure".into());
    };
    let records = service.list(256)?;
    assert_eq!(records.len(), 1);
    assert_eq!(*token_id, records[0].id());
    assert_eq!(output.observed_reveal.len(), 43);
    assert!(!error.to_string().contains(&output.observed_reveal));
    Ok(())
}

#[test]
fn non_token_arguments_remain_owned_by_the_backend_cli() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        parse(&arguments(&["--version"]))?,
        TokenCliCommand::NotTokenCommand
    );
    assert_eq!(
        parse(&arguments(&["token", "--help"]))?,
        TokenCliCommand::Help
    );
    Ok(())
}

struct UnknownAfterCommit {
    inner: TestService,
    issue_calls: usize,
}

struct FailingOutput {
    path: PathBuf,
    observed_reveal: String,
}

impl SecretOutput for FailingOutput {
    fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn write_reveal(&mut self, reveal: &str) -> io::Result<()> {
        self.observed_reveal = reveal.to_owned();
        Err(io::Error::other("injected write failure"))
    }
}

impl TokenCliAuthority for UnknownAfterCommit {
    fn list(&mut self, limit: usize) -> Result<Vec<McpTokenMetadata>, TokenAuthorityFailure> {
        self.inner.list(limit).map_err(|_| TokenAuthorityFailure)
    }

    fn issue(
        &mut self,
        request: McpTokenIssueRequest,
    ) -> Result<McpTokenIssued, TokenAuthorityFailure> {
        self.issue_calls += 1;
        let _issued = self
            .inner
            .issue(request)
            .map_err(|_| TokenAuthorityFailure)?;
        Err(TokenAuthorityFailure)
    }
}

impl<C> TokenCliAuthority for McpTokenService<C>
where
    C: McpTokenCatalogPort,
{
    fn list(&mut self, limit: usize) -> Result<Vec<McpTokenMetadata>, TokenAuthorityFailure> {
        McpTokenService::list(self, limit).map_err(|_| TokenAuthorityFailure)
    }

    fn issue(
        &mut self,
        request: McpTokenIssueRequest,
    ) -> Result<McpTokenIssued, TokenAuthorityFailure> {
        McpTokenService::issue(self, request).map_err(|_| TokenAuthorityFailure)
    }
}

type TestService = McpTokenService<MemoryCatalog>;

fn standalone_service() -> TestService {
    McpTokenService::new([0x5a; MCP_TOKEN_DIGEST_BYTES], MemoryCatalog::default())
}

fn options(
    label: &str,
    path: &std::path::Path,
) -> Result<TokenIssueOptions, heterarchy_orchestration_auth::McpTokenLabelError> {
    Ok(TokenIssueOptions {
        label: McpTokenLabel::new(label.to_owned())?,
        output_path: path.to_path_buf(),
    })
}

fn arguments(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

struct TestPath(PathBuf);

impl TestPath {
    fn new(name: &str) -> Self {
        let unique = Uuid::now_v7();
        Self(std::env::temp_dir().join(format!("heterarchy-token-cli-{name}-{unique}")))
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[derive(Default)]
struct MemoryCatalog {
    records: BTreeMap<McpTokenId, RegisteredMcpToken>,
}

impl McpTokenCatalogPort for MemoryCatalog {
    type Error = Infallible;

    fn issue(&mut self, token: RegisteredMcpToken) -> Result<RegisteredMcpToken, Self::Error> {
        self.records.insert(token.id(), token.clone());
        Ok(token)
    }

    fn authenticate(
        &mut self,
        digest: McpTokenDigest,
    ) -> Result<Option<RegisteredMcpToken>, Self::Error> {
        Ok(self
            .records
            .values()
            .find(|record| record.digest() == digest)
            .cloned())
    }

    fn rotate(
        &mut self,
        predecessor: RegisteredMcpToken,
        replacement: RegisteredMcpToken,
    ) -> Result<(RegisteredMcpToken, RegisteredMcpToken), Self::Error> {
        self.records.insert(predecessor.id(), predecessor.clone());
        self.records.insert(replacement.id(), replacement.clone());
        Ok((predecessor, replacement))
    }

    fn delete(
        &mut self,
        id: McpTokenId,
        _expected_revision: McpTokenRevision,
    ) -> Result<(), Self::Error> {
        self.records.remove(&id);
        Ok(())
    }

    fn revoke(
        &mut self,
        _predecessor: RegisteredMcpToken,
        revoked: RegisteredMcpToken,
    ) -> Result<RegisteredMcpToken, Self::Error> {
        self.records.insert(revoked.id(), revoked.clone());
        Ok(revoked)
    }

    fn get(&mut self, id: McpTokenId) -> Result<Option<RegisteredMcpToken>, Self::Error> {
        Ok(self.records.get(&id).cloned())
    }

    fn list(&mut self, limit: usize) -> Result<Vec<RegisteredMcpToken>, Self::Error> {
        Ok(self.records.values().take(limit).cloned().collect())
    }
}
