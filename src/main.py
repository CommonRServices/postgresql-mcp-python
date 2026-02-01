from mcp.server.fastmcp import FastMCP

from app_context import lifespan
from tools.schema import get_schema
from tools.select_query import execute_select
from tools.write_query import execute_write


mcp = FastMCP(
    "AgentsDbServer", stateless_http=True, json_response=True, lifespan=lifespan
)


# Register database tools
mcp.tool()(get_schema)
mcp.tool()(execute_select)
mcp.tool()(execute_write)


@mcp.tool()
def greet(name: str = "World") -> str:
    """Greet someone by name."""
    return f"Hello, {name}!"


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
