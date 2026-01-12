#!/usr/bin/env python3
"""
Database Migration Script - Migrates old schema to v2.0

This script migrates the database from v1.0 (refresh_token) to v2.0 (refresh_token_encrypted).
"""

import sqlite3
import sys
import os
from pathlib import Path

def check_database_schema(db_path: str) -> dict:
    """Check current database schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get table info
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    # Check token_store columns
    schema_info = {
        "tables": tables,
        "has_old_column": False,
        "has_new_column": False,
        "has_data": False
    }
    
    if "token_store" in tables:
        cursor.execute("PRAGMA table_info(token_store)")
        columns = {row[1]: row for row in cursor.fetchall()}
        
        schema_info["has_old_column"] = "refresh_token" in columns
        schema_info["has_new_column"] = "refresh_token_encrypted" in columns
        
        # Check for existing data
        cursor.execute("SELECT COUNT(*) FROM token_store")
        count = cursor.fetchone()[0]
        schema_info["has_data"] = count > 0
    
    conn.close()
    return schema_info

def migrate_database(db_path: str, secret_key: str = None):
    """Migrate database from v1.0 to v2.0 schema."""
    from cryptography.fernet import Fernet
    
    print(f"📊 Checking database: {db_path}")
    
    if not os.path.exists(db_path):
        print(f"❌ Database file not found: {db_path}")
        return False
    
    schema_info = check_database_schema(db_path)
    
    print(f"   Tables found: {', '.join(schema_info['tables'])}")
    
    # If already using new schema, nothing to do
    if schema_info["has_new_column"] and not schema_info["has_old_column"]:
        print("✅ Database is already using v2.0 schema (refresh_token_encrypted)")
        return True
    
    # If neither column exists, just create new schema
    if not schema_info["has_old_column"] and not schema_info["has_new_column"]:
        print("✅ Database will be created with v2.0 schema on next app start")
        return True
    
    # Need to migrate
    if schema_info["has_old_column"]:
        print(f"🔄 Found old schema (refresh_token column)")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get old token if exists
        old_token = None
        if schema_info["has_data"]:
            try:
                cursor.execute("SELECT refresh_token FROM token_store LIMIT 1")
                result = cursor.fetchone()
                if result and result[0]:
                    old_token = result[0]
                    print(f"   Found existing refresh token (will migrate)")
            except sqlite3.OperationalError:
                pass
        
        # Create new column if it doesn't exist
        if not schema_info["has_new_column"]:
            print("   Adding refresh_token_encrypted column...")
            cursor.execute("""
                ALTER TABLE token_store 
                ADD COLUMN refresh_token_encrypted TEXT
            """)
        
        # Migrate data if we have a token
        if old_token and secret_key:
            print("   Encrypting and migrating token...")
            try:
                # Get encryption key
                if isinstance(secret_key, str):
                    if len(secret_key) == 44:  # Fernet key is base64 encoded, 44 chars
                        key = secret_key.encode()
                    else:
                        key = Fernet.generate_key()
                        print("   ⚠️  SECRET_KEY invalid format, generated new key")
                        print(f"   📝 Update your .env with: SECRET_KEY={key.decode()}")
                else:
                    key = Fernet.generate_key()
                    print("   ⚠️  No SECRET_KEY provided, generated new key")
                    print(f"   📝 Add to .env: SECRET_KEY={key.decode()}")
                
                cipher = Fernet(key)
                encrypted_token = cipher.encrypt(old_token.encode()).decode()
                
                # Update the row
                cursor.execute("""
                    UPDATE token_store 
                    SET refresh_token_encrypted = ? 
                    WHERE refresh_token_encrypted IS NULL
                """, (encrypted_token,))
                
                print("   ✅ Token encrypted and migrated")
            except Exception as e:
                print(f"   ⚠️  Could not encrypt token: {e}")
                print("   ℹ️  Old token will be lost (you'll need to re-authenticate)")
        
        # Drop old column (SQLite doesn't support DROP COLUMN directly)
        print("   Removing old refresh_token column...")
        cursor.execute("""
            CREATE TABLE token_store_new (
                id INTEGER PRIMARY KEY,
                refresh_token_encrypted TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            INSERT INTO token_store_new (id, refresh_token_encrypted, updated_at)
            SELECT id, refresh_token_encrypted, updated_at FROM token_store
        """)
        
        cursor.execute("DROP TABLE token_store")
        cursor.execute("ALTER TABLE token_store_new RENAME TO token_store")
        
        conn.commit()
        conn.close()
        
        print("✅ Migration completed successfully!")
        if old_token and not secret_key:
            print("\n⚠️  IMPORTANT: You need to add SECRET_KEY to your .env file")
            print("   Generate one with: python3 -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"")
        return True
    
    return False

def main():
    """Main migration function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate database from v1.0 to v2.0")
    parser.add_argument("--db", default="jobs.db", help="Database file path")
    parser.add_argument("--secret-key", help="SECRET_KEY for encryption (from .env)")
    parser.add_argument("--backup", action="store_true", help="Create backup before migration")
    
    args = parser.parse_args()
    
    db_path = args.db
    if not os.path.isabs(db_path):
        # Assume it's in the current directory or production directory
        if os.path.exists(os.path.join("production", db_path)):
            db_path = os.path.join("production", db_path)
        elif not os.path.exists(db_path):
            db_path = os.path.join("production", db_path)
    
    # Create backup if requested
    if args.backup and os.path.exists(db_path):
        backup_path = f"{db_path}.backup"
        import shutil
        shutil.copy2(db_path, backup_path)
        print(f"💾 Backup created: {backup_path}")
    
    # Try to load SECRET_KEY from .env if not provided
    secret_key = args.secret_key
    if not secret_key:
        try:
            from dotenv import load_dotenv
            load_dotenv()
            secret_key = os.getenv("SECRET_KEY")
        except:
            pass
    
    success = migrate_database(db_path, secret_key)
    
    if success:
        print("\n✅ Migration complete! You can now start the application.")
        print("   Run: uvicorn app:app --reload")
    else:
        print("\n❌ Migration failed. Check errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
