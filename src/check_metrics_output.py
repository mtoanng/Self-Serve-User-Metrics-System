import duckdb

# === CONFIG ===
DUCKDB_PATH = "metrics.duckdb"

# === Connect to DuckDB ===
con = duckdb.connect(DUCKDB_PATH)

def check_metrics():
    # Get all tables in the database
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]

    if not tables:
        print("⚠️ No tables found in DuckDB. Did you run execute_metrics.py?")
        return

    print(f"📊 Checking {len(tables)} tables in {DUCKDB_PATH}...\n")

    for table in tables:
        try:
            # Count rows
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            # Peek at first few rows
            preview = con.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()

            if count == 0:
                print(f"⚠️ {table}: table exists but has no rows.")
            else:
                print(f"✅ {table}: {count} rows")
                print(preview.to_string(index=False))
                print("-" * 60)
        except Exception as e:
            print(f"❌ Error reading table {table}: {e}")

    print("🏁 Metrics check complete.")

if __name__ == "__main__":
    check_metrics()
