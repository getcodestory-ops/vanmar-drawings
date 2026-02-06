#!/usr/bin/env python3
"""
Multi-user schema migration.

Adds users, sessions tables and user_id/created_by_name columns to
token_store, scheduled_jobs, and jobs. For existing databases with data,
creates a "Legacy" user and assigns existing token/schedules/jobs to it
so automations keep working until users re-login.
"""

import os
import sys
import uuid

# Run from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.sql import text


def get_engine():
    from dotenv import load_dotenv
    load_dotenv()
    from sqlalchemy import create_engine
    from config import settings
    connect_args = {}
    if settings.DATABASE_URL.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    elif settings.DATABASE_URL.startswith("postgresql"):
        if "sslmode" not in settings.DATABASE_URL:
            connect_args["sslmode"] = "require"
    return create_engine(
        settings.DATABASE_URL,
        connect_args=connect_args,
        pool_pre_ping=True,
    )


def column_exists(conn, table: str, column: str, sqlite: bool) -> bool:
    if sqlite:
        r = conn.execute(text(f"PRAGMA table_info({table})"))
        return any(row[1] == column for row in r.fetchall())
    else:
        r = conn.execute(
            text("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :t AND column_name = :c
            """),
            {"t": table, "c": column}
        )
        return r.fetchone() is not None


def table_exists(conn, table: str, sqlite: bool) -> bool:
    if sqlite:
        r = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:t"),
            {"t": table}
        )
        return r.fetchone() is not None
    else:
        r = conn.execute(
            text("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :t
            """),
            {"t": table}
        )
        return r.fetchone() is not None


def run_migration():
    # #region agent log
    print("[MIGRATION] Starting add_multi_user migration")
    try:
        _logpath = "/Users/robertocervantes/Documents/CodeStory/.cursor/debug.log"
        with open(_logpath, "a") as _f:
            import json, time
            _f.write(json.dumps({"message": "MIGRATION_START", "hypothesisId": "H3", "timestamp": time.time()}) + "\n")
    except Exception:
        pass
    # #endregion
    engine = get_engine()
    sqlite = "sqlite" in engine.url.drivername
    print(f"[MIGRATION] Database: {'sqlite' if sqlite else 'postgresql'}")

    with engine.connect() as conn:
        # 1) Create users table if not exists (app may have already via create_all)
        print("[MIGRATION] Step 1: users table")
        if not table_exists(conn, "users", sqlite):
            print("Creating users table...")
            if sqlite:
                conn.execute(text("""
                    CREATE TABLE users (
                        id VARCHAR PRIMARY KEY,
                        procore_user_id VARCHAR UNIQUE,
                        email VARCHAR UNIQUE,
                        name VARCHAR,
                        is_admin BOOLEAN DEFAULT 0,
                        is_active BOOLEAN DEFAULT 1,
                        created_at DATETIME,
                        last_login DATETIME
                    )
                """))
            else:
                conn.execute(text("""
                    CREATE TABLE users (
                        id VARCHAR PRIMARY KEY,
                        procore_user_id VARCHAR UNIQUE,
                        email VARCHAR UNIQUE,
                        name VARCHAR,
                        is_admin BOOLEAN DEFAULT FALSE,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP,
                        last_login TIMESTAMP
                    )
                """))
            conn.commit()

        # 2) Create sessions table if not exists
        print("[MIGRATION] Step 2: sessions table")
        if not table_exists(conn, "sessions", sqlite):
            print("Creating sessions table...")
            if sqlite:
                conn.execute(text("""
                    CREATE TABLE sessions (
                        id VARCHAR PRIMARY KEY,
                        user_id VARCHAR,
                        access_token TEXT,
                        created_at DATETIME,
                        expires_at DATETIME,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """))
            else:
                conn.execute(text("""
                    CREATE TABLE sessions (
                        id VARCHAR PRIMARY KEY,
                        user_id VARCHAR REFERENCES users(id),
                        access_token TEXT,
                        created_at TIMESTAMP,
                        expires_at TIMESTAMP
                    )
                """))
            conn.commit()

        # 3) Add user_id, created_by_name to jobs
        print("[MIGRATION] Step 3: jobs columns")
        for col, typ in (("user_id", "VARCHAR" if sqlite else "VARCHAR"), ("created_by_name", "VARCHAR" if sqlite else "VARCHAR")):
            if not column_exists(conn, "jobs", col, sqlite):
                print(f"Adding jobs.{col}...")
                conn.execute(text(f"ALTER TABLE jobs ADD COLUMN {col} {typ}"))
                conn.commit()

        # 4) Add user_id to token_store
        print("[MIGRATION] Step 4: token_store.user_id")
        tok_user_exists = column_exists(conn, "token_store", "user_id", sqlite)
        print(f"[MIGRATION] token_store.user_id column_exists={tok_user_exists}")
        if not tok_user_exists:
            print("[MIGRATION] Adding token_store.user_id...")
            conn.execute(text("ALTER TABLE token_store ADD COLUMN user_id VARCHAR"))
            conn.commit()
            print("[MIGRATION] token_store.user_id added successfully")

        # 5) Add user_id, created_by_name to scheduled_jobs
        print("[MIGRATION] Step 5: scheduled_jobs columns")
        for col in ("user_id", "created_by_name"):
            if not column_exists(conn, "scheduled_jobs", col, sqlite):
                print(f"Adding scheduled_jobs.{col}...")
                conn.execute(text(f"ALTER TABLE scheduled_jobs ADD COLUMN {col} VARCHAR"))
                conn.commit()

        # 6) If there are token_store rows with user_id NULL, create Legacy user and assign
        print("[MIGRATION] Step 6: Legacy user")
        r = conn.execute(text("SELECT COUNT(*) FROM token_store WHERE user_id IS NULL"))
        null_count = r.scalar() or 0
        if null_count > 0:
            legacy_id = str(uuid.uuid4())
            print("Creating Legacy user for existing token/schedules/jobs...")
            if sqlite:
                conn.execute(
                    text("""
                        INSERT INTO users (id, procore_user_id, email, name, is_admin, is_active, created_at)
                        VALUES (:id, 'legacy', 'legacy@migrated.local', 'Legacy (migrated)', 1, 1, datetime('now'))
                    """),
                    {"id": legacy_id}
                )
            else:
                conn.execute(
                    text("""
                        INSERT INTO users (id, procore_user_id, email, name, is_admin, is_active, created_at)
                        VALUES (:id, 'legacy', 'legacy@migrated.local', 'Legacy (migrated)', TRUE, TRUE, NOW())
                    """),
                    {"id": legacy_id}
                )
            conn.execute(text("UPDATE token_store SET user_id = :id WHERE user_id IS NULL"), {"id": legacy_id})
            conn.execute(text("UPDATE scheduled_jobs SET user_id = :id WHERE user_id IS NULL"), {"id": legacy_id})
            conn.execute(text("UPDATE jobs SET user_id = :id WHERE user_id IS NULL"), {"id": legacy_id})
            conn.commit()
            print("Legacy user created. Existing automations will run under this user until users re-login.")

    print("Multi-user migration completed.")


if __name__ == "__main__":
    run_migration()
