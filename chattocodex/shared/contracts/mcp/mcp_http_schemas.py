from typing import Annotated, Literal

from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field


class McpHealthResponse(StrictSchemaModel):
    """Describe the stable MCP process health response."""

    ok: Annotated[
        Literal[True],
        described_field(description="Whether the MCP process is healthy."),
    ] = True
    name: Annotated[
        str,
        described_field(
            description="Stable service name exposed by the MCP health route."
        ),
    ] = "chattocodex"
