from mcp import types
from mcp.server.mcpserver import Context, MCPServer
from pydantic import JsonValue


class ConnectorQualifiedMCPServer(MCPServer[None]):
    """Accept connector-qualified aliases for already-registered MCP tools."""

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, JsonValue],
        context: Context[None, JsonValue] | None = None,
    ) -> types.CallToolResult | types.InputRequiredResult:
        """Call tool. Args: name, arguments, context. Returns: types.CallToolResult | types.InputRequiredResult."""
        resolved_name = await self._resolve_tool_name(name=name)
        return await super().call_tool(resolved_name, arguments, context)

    async def _resolve_tool_name(self, name: str) -> str:
        """Resolve tool name. Args: name. Returns: str."""
        if "." not in name:
            return name

        registered_names = {tool.name for tool in await self.list_tools()}
        if name in registered_names:
            return name

        qualifier, separator, candidate = name.rpartition(".")
        if (
            qualifier
            and "." not in qualifier
            and separator
            and candidate in registered_names
        ):
            return candidate
        return name
