# Heterarchy ChatGPT Companion

This Manifest V3 extension is the provider/browser adapter for the durable
`AgentProvision` lifecycle. It is intentionally not an orchestration authority.

## Packaged installation

The macOS app supplies the native host at
`/Applications/Heterarchy Orchestration.app/Contents/MacOS/heterarchy-orchestration-mcp`
and the unpacked extension at
`/Applications/Heterarchy Orchestration.app/Contents/Resources/companion`.
Use these installed paths together. Registering a Cargo target binary or a
separate dated runtime copy leaves Chrome on an older backend after an app update.

Generate the Native Messaging manifest with the installed bundled executable
using the `native-companion manifest` command below. Its absolute executable
path is resolved at generation time, so do not generate a production
registration from temporary package staging. The manifest command preserves
an existing output file; inspect any existing registration before replacing it.
The package itself does not change Chrome permissions or registrations.

The extension:

- talks only to the `com.heterarchy.orchestration` Chrome Native Messaging host;
- asks the backend for the single admitted durable provision operation;
- opens browser surfaces only for an explicit admitted `execute` dispatch; idle
  polling and reconnects cannot open or wake a tab;
- owns one unfocused/minimized background Chrome window and keeps runtime tabs
  inside it instead of attaching them to arbitrary user windows;
- treats tab/window/document identity as ephemeral browser-session state: a
  closed tab does not delete the durable RuntimeSession or Agent Profile;
- reuses a still-owned exact RuntimeSession conversation surface before opening
  another tab, and a wake never creates a new ChatGPT conversation;
- never closes, overwrites, adopts, or reuses a pinned/user-owned tab;
- fences browser writes by operation revision, tab, exact Chrome document ID,
  and navigation epoch;
- injects only a generic runtime bootstrap packet, never the AgentRequest
  objective, role, capability, Goal/DAG, Writer Lease, or leadership state;
- keeps durable browser evidence / lost-ack reconciliation in
  `chrome.storage.local`, while ephemeral window/tab ownership lives only in
  `chrome.storage.session`; durable journal tab/document readback is additionally
  fenced to the exact browser-session identity, so restarted Chrome cannot alias
  a reused numeric tab ID to unrelated user state;
- reports an uncertain post-submit result as `effect_unknown` and never retries
  the provider effect under a new operation identity.

## Development loading

The checked-in manifest contains only a public development key. It fixes the
unpacked-extension identity across isolated Chrome profiles without storing a
private signing key. The corresponding development extension ID is:

```text
bfamlphchpldhbbegcmjlnhlhafheiag
```

Changing or removing the checked-in public key changes provider admission
identity and therefore requires deliberately regenerating the Native Messaging
host manifest. Do not treat a profile-assigned extension ID as durable authority.

1. For normal interactive development, load this directory as an unpacked
   extension from `chrome://extensions` and verify the ID above.
2. Run the installed MCP binary explicitly:

   ```text
   heterarchy-orchestration-mcp native-companion manifest bfamlphchpldhbbegcmjlnhlhafheiag <absolute-output-path>
   ```

   The command resolves its own executable path and writes a new manifest with
   the exact extension origin. It refuses a relative output path, malformed
   extension ID, or an already-existing output file.
3. Install the generated native-host manifest in Chrome's platform Native Messaging host
   location.

### Native host not found in normal macOS Chrome

`Specified native messaging host not found` is a host-discovery failure, not a
Workspace registration error. Check the exact `com.heterarchy.orchestration`
host name, valid manifest JSON, the browser's manifest directory, and the
absolute executable path. The extension's manifest and the native-host manifest
are different files; installing the app or loading the extension does not
register the native host automatically.

For normal Google Chrome with its default user-data directory, the user-level
host file is:

```text
~/Library/Application Support/Google/Chrome/NativeMessagingHosts/com.heterarchy.orchestration.json
```

After confirming the installed executable and the extension ID above, the user
can generate a missing registration directly with:

```sh
HOST="/Applications/Heterarchy Orchestration.app/Contents/MacOS/heterarchy-orchestration-mcp"
MANIFEST_DIR="$HOME/Library/Application Support/Google/Chrome/NativeMessagingHosts"
test -x "$HOST" && mkdir -p "$MANIFEST_DIR" && \
  "$HOST" native-companion manifest bfamlphchpldhbbegcmjlnhlhafheiag \
  "$MANIFEST_DIR/com.heterarchy.orchestration.json"
```

This deliberately does not overwrite an existing registration. Inspect an
existing file before replacing it; do not delete Workspace state, change the
extension allowlist, or point production registration at a temporary build.
Chrome for Testing, Chromium and custom user-data directories use different
lookup locations. Reload the extension after the registration is corrected.
Existing prepared AgentRequest operations should be inspected and resumed,
not replaced with new requests solely because the host was unavailable.

For automated isolated E2E, use Chrome for Testing rather than relying on
command-line unpacked-extension flags in branded Google Chrome. Start Chrome
for Testing with a dedicated `--user-data-dir`, load this directory there, and
place the generated manifest under that test profile's `NativeMessagingHosts`
directory. Also set `HETERARCHY_FORGE_REGISTRY_PATH` on the Chrome for Testing
process to a test-only registry path so the native host it launches cannot open
normal user state. This keeps extension state, Native Messaging registration,
and Operations/Profile catalogs out of the user's normal runtime.

The checked-in host manifest is a template on purpose. A host manifest with a
guessed extension ID or repository-relative executable path is not valid
provider authority.


## Inspect a failed provision

The native host records a checked, static `failure_code` with the terminal
Failed transition. A lost acknowledgement can replay only the original
revision and code; a changed reason never overwrites the first observation.
Older failures with no recorded reason remain unknown rather than receiving
an invented cause. Raw browser errors, page content and prompts are not stored
in these diagnostic fields.

After the host has migrated its Operations catalog to v33, inspect one exact
operation through the same installed binary:

```text
heterarchy-orchestration-mcp native-companion inspect <operation-id>
```

This local command reads an existing catalog without creating/migrating it,
opening Profile state, acquiring Broker authority, or dispatching a provider
effect. It prints only operation/request IDs, lifecycle state, revision and the
browser-reported static reason. A missing operation prints `null`; an old or
damaged catalog returns an error. `confirmed` is browser-effect evidence, not
proof that Agent activation reached Ready.

Upgrade the native host and other Operations catalog consumers before reloading
the companion that reports failure codes. Unknown post-click outcomes remain
Inspect-only; diagnostics never authorize resending under a new identity.
