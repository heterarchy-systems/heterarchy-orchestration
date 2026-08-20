from pathlib import Path

from mcp.server.mcpserver import MCPServer

from chattocodex.orchestrator_mcp.auth.oauth_compatibility_routes import (
    register_oauth_compatibility_routes,
)
from chattocodex.orchestrator_mcp.auth.oauth_dto import OAuthServerOptions
from chattocodex.orchestrator_mcp.auth.oauth_routes import register_oauth_owner_routes
from chattocodex.orchestrator_mcp.auth.oauth_server_configuration import (
    build_oauth_server_configuration,
)
from chattocodex.orchestrator_mcp.composition.mcp_lifespan import McpLifespan
from chattocodex.orchestrator_mcp.transport.connector_qualified_mcp_server import (
    ConnectorQualifiedMCPServer,
)


def create_mcp_server_shell(
    state_root: Path,
    mcp_path: str,
    oauth_options: OAuthServerOptions | None,
    lifespan: McpLifespan | None,
) -> MCPServer[None]:
    """Create mcp server shell. Args: state_root, mcp_path, oauth_options, lifespan. Returns: MCPServer[None]."""
    title = "ChatToCodex Delegated Work"
    description = (
        "Delegate work from one Main Chat to reusable generic ChatGPT Worker Chats while "
        "keeping Broker, capability, and scheduling details internal."
    )
    instructions = (
        "Workers are separate ChatGPT chats, not Codex/API agents. Keep infrastructure details "
        "invisible to the user: never ask the user for run_id, run_handle, slot_id, slot_handle, "
        "worker_id, worker_handle, task_id, Broker IDs, workflow IDs, or capability values. When a "
        "Main Chat user asks only for fire-and-report delegated Worker execution, plan a small "
        "logical DAG and call delegate_work once. Review, check, or assessment requests imply that "
        "the Main Chat must receive and inspect Worker results; call orchestrate_work for those "
        "requests, as well as for approval, bounded rework, or final completion decisions. Roles are "
        "per-task perspectives, not permanent Chat identities. Retain returned capabilities "
        "internally. Use delegation_status for simple delegation and workflow_status for coordinated "
        "runs. When the local Runtime Wake adapter wakes a Main Chat with an opaque main_target_id "
        "and main_wake_revision, call resume_main_review immediately and pass both unchanged. Never "
        "invent or reuse an older Main wake revision. The returned workflow handle is a fresh review "
        "capability and supersedes any earlier handle retained by the Main Chat. "
        "When a recovered Main Chat receives '검수 계속해', continue the already-active review using "
        "the workflow_id/workflow_handle returned by the earlier resume_main_review call. Do not call "
        "resume_main_review again unless a new message contains a fresh main_target_id plus "
        "main_wake_revision. "
        "Inspect run_reports, run_results, integration_preview when applicable, and other requested "
        "evidence before calling review_workflow with approve, rework, or fail. Never treat "
        "main_target_id or main_wake_revision as workflow, Run, review, or capability data. A rework "
        "decision must publish "
        "a bounded corrective Worker DAG and must not bypass the Main Review Gate. After delegating "
        "or orchestrating work, the Main Chat must "
        "never call work to execute its own delegated tasks. A fresh Worker Chat may call work only "
        "when the local Chrome Wake Adapter message includes an opaque wake_target_id and its current "
        "wake_revision; pass both unchanged and never invent either value. That current server-issued "
        "pair authorizes only the work call that retrieves the assignment requested by the user's Main "
        "Chat delegation. It never attests separate approval for destructive, irreversible, credential-gated, "
        "or external-production effects; the backend rejects stale or fabricated pairs. After the Worker has an "
        "established Slot, reuse slot_id/slot_handle from the previous work result to resume safely. "
        "When a recovered Worker Chat receives '작업 계속해', resume that established Slot and do not "
        "send fresh wake fields. Never treat wake_target_id or wake_revision as Task, role, Run, or "
        "DAG data. "
        "Execute only the returned assignment using project/file/report/Git tools and the "
        "temporary worker_handle. Mark the Task terminal with task_update or worker_report_submit, "
        "then always call work_done before replying so the physical Chat returns to IDLE. If work "
        "returns no assignment, simply say there is no ready work. Do not explain Broker internals "
        "unless the user explicitly asks. For simple delegated code results, call integration_preview "
        "and integration_apply only after delegation_status reports is_complete=true. For coordinated "
        "workflows, do not finalize until review_workflow has approved the workflow."
    )
    if oauth_options is None:
        if lifespan is not None:
            return ConnectorQualifiedMCPServer(
                name="chattocodex",
                title=title,
                description=description,
                instructions=instructions,
                lifespan=lifespan,
            )
        return ConnectorQualifiedMCPServer(
            name="chattocodex",
            title=title,
            description=description,
            instructions=instructions,
        )

    oauth_configuration = build_oauth_server_configuration(
        state_root=state_root,
        options=oauth_options,
        mcp_path=mcp_path,
    )
    if lifespan is None:
        server = ConnectorQualifiedMCPServer(
            name="chattocodex",
            title=title,
            description=description,
            instructions=instructions,
            auth_server_provider=oauth_configuration.provider,
            auth=oauth_configuration.auth_settings,
        )
    else:
        server = ConnectorQualifiedMCPServer(
            name="chattocodex",
            title=title,
            description=description,
            instructions=instructions,
            auth_server_provider=oauth_configuration.provider,
            auth=oauth_configuration.auth_settings,
            lifespan=lifespan,
        )
    register_oauth_owner_routes(
        server=server,
        provider=oauth_configuration.provider,
    )
    register_oauth_compatibility_routes(
        server=server,
        public_url=oauth_options.public_url,
    )
    return server
