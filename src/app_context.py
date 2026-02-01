from dataclasses import dataclass
from contextlib import asynccontextmanager

from mcp.server.fastmcp import FastMCP
from asyncpg import Pool

from db import init_db_pool


@dataclass
class AppContext:
    db_pool: Pool


@asynccontextmanager
async def lifespan(server: FastMCP):
    pool = await init_db_pool()
    try:
        yield AppContext(db_pool=pool)
    finally:
        await pool.close()
