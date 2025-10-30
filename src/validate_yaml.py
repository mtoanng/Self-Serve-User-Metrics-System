import os
import yaml
import duckdb

con = duckdb.connect('metrics.duckdb')

# Define required fiels
REQUIRED_FIELDS = ["metric_name", "description", "owner", "schedule", "sql"]

def validate_yaml_fields(yaml_data, file_path):
    """Check required fields exist and are not empty."""
    missing = [f for f in REQUIRED_FIELDS if f not in yaml_data]
    empty = [f for f in REQUIRED_FIELDS if not yaml_data.get(f)]

    errors = []
    if missing:
        errors.append(f"Missing fields: {', '.join(missing)}")
    if empty:
        errors.append(f"Empty fields: {', '.join(empty)}")

    if errors:
        print(f"❌ {file_path}: " + " | ".join(errors))
        return False
    return True


def test_sql(con, sql, file_path):
    """Test SQL syntax and executability safely."""
    try:
        clean_sql = sql.strip().rstrip(";").strip()
        
        # Check if SQL already has a LIMIT clause
        has_limit = "limit" in clean_sql.lower()
        
        # Run EXPLAIN to verify syntax first
        con.execute(f"EXPLAIN {clean_sql}")
        
        # If it already has LIMIT, run as-is; otherwise, append LIMIT 1 for a quick test
        test_sql = clean_sql if has_limit else f"{clean_sql} LIMIT 1"
        con.execute(test_sql)
        
        print(f"✅ SQL check passed for {file_path}")
        return True
    except Exception as e:
        print(f"❌ SQL error in {file_path}: {e}")
        return False


def validate_yaml_file(file_path):
    """Validate a single YAML file."""
    try:
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        if not data:
            print(f"❌ {file_path} is empty or invalid YAML.")
            return False

        valid_fields = validate_yaml_fields(data, file_path)
        valid_sql = test_sql(con, data.get("sql", ""), file_path) if valid_fields else False

        return valid_fields and valid_sql
    except yaml.YAMLError as e:
        print(f"❌ YAML parsing error in {file_path}: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error in {file_path}: {e}")
        return False


def main(metrics_folder="metrics"):
    """Validate all YAML files in the metrics folder."""
    yaml_files = [f for f in os.listdir(metrics_folder) if f.endswith(".yaml")]

    print(f"\n🔍 Validating {len(yaml_files)} YAML metric files in '{metrics_folder}'...\n")
    all_passed = True

    for file_name in yaml_files:
        file_path = os.path.join(metrics_folder, file_name)
        if not validate_yaml_file(file_path):
            all_passed = False

    print("\n✅ All checks passed!" if all_passed else "\n❌ Some metrics failed validation.")


if __name__ == "__main__":
    main()
