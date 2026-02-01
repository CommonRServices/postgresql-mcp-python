from mcp.server.fastmcp import Context

from app_context import AppContext


async def execute_write(
    ctx: Context,
    query: str,
    params: list | None = None,
) -> dict:
    """
    Execute INSERT, UPDATE, or DELETE queries on the PostgreSQL database.

    This tool allows data modification operations.
    SELECT queries should use the execute_select tool instead.

    Args:
        query: The SQL query to execute (INSERT, UPDATE, or DELETE).
        params: Optional list of parameters for parameterized queries.
               Use $1, $2, etc. as placeholders in the query.

    Returns:
        Dictionary containing the operation result with affected row count.

    Examples:
        execute_write("INSERT INTO users (name, email) VALUES ($1, $2)", ["John", "john@example.com"])
        execute_write("UPDATE users SET name = $1 WHERE id = $2", ["Jane", 1])
        execute_write("DELETE FROM users WHERE id = $1", [1])
    """
    app_ctx: AppContext = ctx.request_context.lifespan_context

    # Validate that it's a write query
    normalized_query = query.strip().upper()
    allowed_operations = ["INSERT", "UPDATE", "DELETE"]

    is_allowed = any(normalized_query.startswith(op) for op in allowed_operations)
    if not is_allowed:
        return {
            "success": False,
            "error": "Only INSERT, UPDATE, or DELETE queries are allowed. Use execute_select for SELECT queries.",
            "affected_rows": 0,
        }

    # Block dangerous DDL operations
    dangerous_keywords = ["DROP", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE"]
    for keyword in dangerous_keywords:
        if keyword in normalized_query.split():
            return {
                "success": False,
                "error": f"Query contains forbidden keyword: {keyword}. Only INSERT, UPDATE, and DELETE are allowed.",
                "affected_rows": 0,
            }

    async with app_ctx.db_pool.acquire() as conn:
        try:
            if params:
                result = await conn.execute(query, *params)
            else:
                result = await conn.execute(query)

            # Parse the result to get affected rows count
            # Result format is like "INSERT 0 1" or "UPDATE 5" or "DELETE 3"
            parts = result.split()
            affected_rows = 0
            if len(parts) >= 2:
                try:
                    # For INSERT, the count is the last part
                    # For UPDATE/DELETE, it's also the last part
                    affected_rows = int(parts[-1])
                except ValueError:
                    affected_rows = 0

            operation = parts[0] if parts else "UNKNOWN"

            return {
                "success": True,
                "operation": operation,
                "affected_rows": affected_rows,
                "message": f"{operation} operation completed successfully. {affected_rows} row(s) affected.",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "affected_rows": 0,
            }
