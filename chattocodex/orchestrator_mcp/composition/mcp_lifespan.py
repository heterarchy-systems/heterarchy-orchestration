from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from mcp.server.mcpserver import MCPServer

McpLifespan = Callable[[MCPServer[None]], AbstractAsyncContextManager[None]]
