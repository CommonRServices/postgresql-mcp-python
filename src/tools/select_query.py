from mcp.server.fastmcp import Context

from app_context import AppContext


async def execute_select(
    ctx: Context,
    query: str,
    params: list | None = None,
) -> dict:
    """
    Execute a SELECT query on the PostgreSQL database.

    This tool only allows SELECT queries for reading data.
    Any non-SELECT query will be rejected.

    Args:
        query: The SELECT SQL query to execute.
        params: Optional list of parameters for parameterized queries.
               Use $1, $2, etc. as placeholders in the query.

    Returns:
        Dictionary containing the query results with rows and metadata.

    Example:
        execute_select("SELECT * FROM users WHERE id = $1", [1])
    """
    app_ctx: AppContext = ctx.request_context.lifespan_context

    # Validate that it's a SELECT query
    normalized_query = query.strip().upper()
    if not normalized_query.startswith("SELECT"):
        return {
            "success": False,
            "error": "Only SELECT queries are allowed. Use execute_write for INSERT, UPDATE, or DELETE.",
            "rows": [],
            "row_count": 0,
        }

    # Check for dangerous operations that might be embedded
    dangerous_keywords = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "TRUNCATE",
        "ALTER",
        "CREATE",
    ]
    # Skip the first word (SELECT) and check for dangerous keywords
    query_body = normalized_query[6:]
    for keyword in dangerous_keywords:
        if keyword in query_body.split():
            return {
                "success": False,
                "error": f"Query contains forbidden keyword: {keyword}. Only pure SELECT queries are allowed.",
                "rows": [],
                "row_count": 0,
            }

    async with app_ctx.db_pool.acquire() as conn:
        try:
            if params:
                rows = await conn.fetch(query, *params)
            else:
                rows = await conn.fetch(query)

            # Convert rows to list of dicts
            result_rows = [dict(row) for row in rows]

            return {
                "success": True,
                "rows": result_rows,
                "row_count": len(result_rows),
                "columns": list(result_rows[0].keys()) if result_rows else [],
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "rows": [],
                "row_count": 0,
            }
