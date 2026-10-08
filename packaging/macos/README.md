# macOS headless backend package

The package contains one independently runnable Rust daemon:
`heterarchy-orchestration-backend`. It has no GUI or browser startup
dependency. The package includes the daemon, a launchd template, an
environment reference file, and this guide.

## Configure the user-owned service

Run the daemon as the same macOS user who bootstraps its access token. The
native token authority uses that user's existing application-support catalog
and Keychain item. A system LaunchDaemon running as `root` has a different
home directory and Keychain identity, so use a per-user LaunchAgent for this
configuration.

Create user-owned service paths and restrict them to the account:

```sh
mkdir -p "$HOME/.local/share/heterarchy-orchestration"
mkdir -p "$HOME/.config/heterarchy-orchestration"
chmod 700 "$HOME/.local/share/heterarchy-orchestration" \
    "$HOME/.config/heterarchy-orchestration"
```

Copy `backend.env.example` as a reference for the variables the daemon
understands. It is not loaded automatically. launchd does not read `.env`
files: put each needed key directly in the plist's `EnvironmentVariables`
dictionary, or use an operator-maintained wrapper that explicitly reads the
file. Keep configuration files user-owned and mode `0600`; do not put bearer
secrets in environment variables.

The `HETERARCHY_ORCHESTRATION_DATA_DIR` setting selects the daemon's
user-owned operational data root. It is separate from the app-global token
catalog and Keychain pepper, which remain owned by the existing native auth
runtime. The launchd template rendered by the package uses `/var/lib` as a
placeholder default. Before loading it as a LaunchAgent, replace the data
root and both log paths with absolute paths under the user's home directory.

## Bootstrap one backend access token

Issue the access token once from the same logged-in account that will own the
LaunchAgent. The required flag makes first-install authority initialization
explicit. The output path must not already exist; the command reserves a
mode-`0600` file before it touches the native token authority.

```sh
TOKEN_FILE="$HOME/.config/heterarchy-orchestration/backend-token"
/usr/local/bin/heterarchy-orchestration-backend token issue \
    --initialize-authority \
    --label "local backend operator" \
    --output "$TOKEN_FILE"
```

The bearer secret is written only to `TOKEN_FILE`. Standard output contains
non-secret metadata, including the token ID used by
`HETERARCHY_ORCHESTRATION_OPERATOR_PRINCIPALS` as `token:<uuid>`. Keep the
token file readable only by its owner and pass its contents to clients only
through their supported secret input. If the command reports an uncertain
outcome, follow its displayed token ID readback guidance and do not rerun the
issue command blindly.

After bootstrap, leave token-authority initialization unset for daemon
startup. The daemon opens the same existing app-global catalog and Keychain
pepper under the owning macOS account.

## Run under launchd

Edit the packaged plist so `HETERARCHY_ORCHESTRATION_DATA_DIR`, `StandardOutPath`,
and `StandardErrorPath` all point to absolute user-owned paths. Add any other
configuration values directly to its `EnvironmentVariables` dictionary;
launchd will not source `backend.env.example`.

Place the edited file at
`$HOME/Library/LaunchAgents/systems.heterarchy.orchestration.backend.plist`,
then load it in the user's GUI launchd domain:

```sh
launchctl bootstrap "gui/$(id -u)" \
    "$HOME/Library/LaunchAgents/systems.heterarchy.orchestration.backend.plist"
```

The job starts at login and launchd restarts it after exit. `SIGTERM` requests
graceful shutdown; `launchctl bootout gui/$(id -u)/systems.heterarchy.orchestration.backend`
stops the job.

The daemon listens on `127.0.0.1:8798` by default. Set
`HETERARCHY_ORCHESTRATION_BACKEND_ADDR` in the plist to select another
loopback address. Health and readiness routes are public; other backend
routes require a bearer token.

## Package contents and local build

The archive installs under `/usr/local`:

```text
/usr/local/bin/heterarchy-orchestration-backend
/usr/local/share/heterarchy-orchestration/launchd/systems.heterarchy.orchestration.backend.plist
/usr/local/share/heterarchy-orchestration/config/backend.env.example
/usr/local/share/heterarchy-orchestration/README.md
/usr/local/SHA256SUMS
```

The package verifier expands the Installer archive and checks every payload
file against the staged `SHA256SUMS` manifest. Build with the repository's
designated SSD launcher:

```sh
../.codex/rust-ssd orchestration make package
```

The default local build uses ad-hoc binary signatures and an unsigned
Installer. Optional signing identities are available outside explicit
output-root mode:

```sh
ORCHESTRATION_APP_SIGNING_IDENTITY="Developer ID Application: Example (TEAMID)" \
ORCHESTRATION_INSTALLER_SIGNING_IDENTITY="Developer ID Installer: Example (TEAMID)" \
../.codex/rust-ssd orchestration make package
```

Outputs are written under `dist/<run-id>/`. Release automation and
distribution notarization are separate work. Each invocation reserves new
staging paths and preserves pre-existing paths; an explicit output root must
have no existing `build`, `work`, or `dist` child.

## Serve logical Session authority to Forge

The same headless executable can serve the existing read-only Session resolver
independently of the public daemon. This replaces the retired MCP launcher.
It does not create a catalog, issue a user access token, or grant work admission.

Select the exact existing logical Session catalog used by the Backend, and a
separate service-to-service issuer secret file. Keep that file canonical,
regular, and private (mode0600 or0400); symlink paths and group/world access
are rejected. The secret is not the Backend user bearer token. Configure
Forge's RemoteAgentSessionAuthority with the matching loopback address and
issuer secret through Forge's supported configuration.

```sh
/usr/local/bin/heterarchy-orchestration-backend session-authority \
  --port 8799 \
  --catalog /absolute/path/to/existing-logical-session-catalog.sqlite3 \
  --issuer-secret-file /absolute/path/to/private-session-issuer-secret
```

The process prints only session_authority_endpoint=http://127.0.0.1:<port>.
Port0 requests an ephemeral listener for isolated verification; use an
explicit configured port for a managed service. The endpoint is authenticated
POST /internal/v1/sessions/resolve, with the versioned Forge session protocol.
It rejects browser Origin headers and does not expose session mutation routes.
Reads compare the exact Session identity and original principal against the
authoritative catalog. SIGTERM and SIGINT stop admission and drain/join the
owned listener and queries; incomplete shutdown returns an error.

Run this command as a separately managed user-owned service when Forge needs
the resolver. Its startup does not use the production Keychain/token catalog.
Resolving an open logical Session alone does not prove Workspace attachment,
coordinator actor authority, Broker fencing, or execution admission.
