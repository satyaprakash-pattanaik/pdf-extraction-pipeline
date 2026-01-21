import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# --------------------------------------------------
# DATABASE URL (runtime)
# --------------------------------------------------
DATABASE_URL = (
    "postgresql://neondb_owner:npg_6wF7xiacSkGd@"
    "ep-odd-boat-adwdup95-pooler.c-2.us-east-1.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)

# --------------------------------------------------
# CONNECT
# --------------------------------------------------
def db_connect():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        connect_timeout=10,
        cursor_factory=RealDictCursor
    )
