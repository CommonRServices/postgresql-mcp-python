from mcp.server.fastmcp import FastMCP

# Stateless server with JSON responses (recommended)
mcp = FastMCP("AgentsDbServer", stateless_http=True, json_response=True)


# Add a simple tool to demonstrate the server
@mcp.tool()
def greet(name: str = "World") -> str:
    """Greet someone by name."""
    return f"Hello, {name}!"


# Run server with streamable_http transport
if __name__ == "__main__":
    mcp.run(transport="streamable-http")