//! MCP bearer-token admission, durable catalog, and bounded workers.
//! The token authority owns its typed model locally; the Forge domain no
//! longer carries these types after the MCP retirement.

mod pepper_store;
mod token;
mod token_catalog;
mod token_model;
mod token_paths;
mod token_runtime;

pub use pepper_store::{MacOsMcpTokenPepperStore, McpTokenPepper, McpTokenPepperError};
pub use token::{
    MAX_MCP_TOKEN_BEARER_BYTES, MAX_MCP_TOKEN_LIST_RESULTS, MCP_TOKEN_QUEUE_CAPACITY,
    MCP_TOKEN_REVEAL_BYTES, MCP_TOKEN_REVEAL_CHARS, McpTokenAuthorization,
    McpTokenCatalog as McpTokenCatalogPort, McpTokenClient, McpTokenClientError,
    McpTokenIssueRequest, McpTokenIssued, McpTokenMetadata, McpTokenRequest, McpTokenReveal,
    McpTokenRotation, McpTokenService, McpTokenServiceError, McpTokenWorker, McpTokenWorkerError,
    PendingMcpToken,
};
pub use token_catalog::{MAX_MCP_TOKEN_CATALOG_RESULTS, McpTokenCatalog, McpTokenCatalogError};
pub use token_model::{
    MAX_MCP_TOKEN_LABEL_BYTES, MCP_TOKEN_DIGEST_BYTES, McpTokenDigest, McpTokenId,
    McpTokenIdParseError, McpTokenLabel, McpTokenLabelError, McpTokenRecordParts, McpTokenRevision,
    McpTokenRevisionError, McpTokenState, McpTokenTransitionError, RegisteredMcpToken,
};
pub use token_paths::{TokenAuthorityPaths, TokenPathError};
pub use token_runtime::{McpTokenBootstrapMode, McpTokenRuntime, McpTokenRuntimeError};
