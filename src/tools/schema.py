from mcp.server.fastmcp import Context

from app_context import AppContext


async def get_schema(
    ctx: Context,
    schema_name: str = "public",
    table_name: str | None = None,
) -> dict:
    """
    Get the PostgreSQL database schema information.

    Returns table names, column names, data types, and constraints
    for the specified schema.

    Args:
        schema_name: The schema to query (default: "public")
        table_name: Optional specific table name to get schema for.
                   If not provided, returns schema for all tables.

    Returns:
        Dictionary containing schema information with tables and their columns.
    """
    app_ctx: AppContext = ctx.request_context.lifespan_context

    async with app_ctx.db_pool.acquire() as conn:
        if table_name:
            # Get schema for a specific table
            query = """
                SELECT
                    c.table_name,
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    tc.constraint_type
                FROM information_schema.columns c
                LEFT JOIN information_schema.key_column_usage kcu
                    ON c.table_name = kcu.table_name
                    AND c.column_name = kcu.column_name
                    AND c.table_schema = kcu.table_schema
                LEFT JOIN information_schema.table_constraints tc
                    ON kcu.constraint_name = tc.constraint_name
                    AND kcu.table_schema = tc.table_schema
                WHERE c.table_schema = $1
                    AND c.table_name = $2
                ORDER BY c.ordinal_position
            """
            rows = await conn.fetch(query, schema_name, table_name)
        else:
            # Get schema for all tables
            query = """
                SELECT
                    c.table_name,
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    tc.constraint_type
                FROM information_schema.columns c
                LEFT JOIN information_schema.key_column_usage kcu
                    ON c.table_name = kcu.table_name
                    AND c.column_name = kcu.column_name
                    AND c.table_schema = kcu.table_schema
                LEFT JOIN information_schema.table_constraints tc
                    ON kcu.constraint_name = tc.constraint_name
                    AND kcu.table_schema = tc.table_schema
                WHERE c.table_schema = $1
                ORDER BY c.table_name, c.ordinal_position
            """
            rows = await conn.fetch(query, schema_name)

        # Organize results by table
        tables: dict = {}
        for row in rows:
            tbl_name = row["table_name"]
            if tbl_name not in tables:
                tables[tbl_name] = {"columns": []}

            tables[tbl_name]["columns"].append(
                {
                    "column_name": row["column_name"],
                    "data_type": row["data_type"],
                    "is_nullable": row["is_nullable"],
                    "column_default": row["column_default"],
                    "max_length": row["character_maximum_length"],
                    "constraint_type": row["constraint_type"],
                }
            )

        return {
            "schema": schema_name,
            "tables": tables,
            "table_count": len(tables),
        }
