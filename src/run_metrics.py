import os
import yaml
import duckdb

METRICS_FOLDER = "metrics"
DUCKDB_PATH = "metrics.duckdb"

# Connect to DB to test SQL
con = duckdb.connect('metrics.duckdb')

def execute_metric(file_path):
    """Read a metric YAML, validate fields, and execute SQL."""
    with open(file_path, "r") as f:
        metric = yaml.safe_load(f)

    # Required fields
    required_fields = ["metric_name", "sql"]
    for field in required_fields:
        if field not in metric or not metric[field]:
            print(f"⚠️ Skipping {file_path}: missing '{field}'")
            return

    metric_name = metric["metric_name"]
    sql = metric["sql"].strip().rstrip(";").strip()  # remove trailing semicolon

    try:
        # Execute SQL and create/replace the metric table
        con.execute(f"CREATE OR REPLACE TABLE {metric_name} AS {sql}")
        print(f"✅ Metric '{metric_name}' successfully created from {os.path.basename(file_path)}")
    except Exception as e:
        print(f"❌ Failed to execute {metric_name}: {e}")

def main():
    yaml_files = [
        os.path.join(METRICS_FOLDER, f)
        for f in os.listdir(METRICS_FOLDER)
        if f.endswith(".yaml")
    ]

    if not yaml_files:
        print("⚠️ No YAML metric files found in /metrics folder.")
        return

    print(f"📊 Executing {len(yaml_files)} metric(s)...")
    for yaml_file in yaml_files:
        execute_metric(yaml_file)

    print("🏁 Done executing all metrics.")

if __name__ == "__main__":
    main()
