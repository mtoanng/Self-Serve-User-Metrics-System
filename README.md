# 🧮 E-commerce Metrics Pipeline (DuckDB + YAML)

This project provides a lightweight, automated framework for defining and executing business metrics using **YAML configuration files** and **DuckDB** as the analytical engine.

The system validates and executes SQL-based metric definitions, materializing each metric as a **table** in DuckDB for easy reuse and analysis.

---

## 1. SETUP INSTRUCTIONS

### 🧰 Prerequisitess
- Python 3.10+  
- pip (Python package manager)
- DuckDB installed via pip

### 📦 Install dependencies
pip install duckdb pyyaml pandas numpy

## 2. RUN SCRIPTS

# a. Load data into DuckDB
python src/load_data.py

# b. Validate YAMLs
python src/validate_yaml.py

# c. Execute Metrics
python src/run_metrics.py

# d. Validate output
python src/check_metrics_output.py

## 3. ADDING A NEW METRIC

You can easily extend this system by adding new .yaml files under /metrics.

# Step 1: Create a new YAML file
Example: /metrics/avg_items_per_order.yaml

# Step 2: Adding the metric fields follow the required schema

| Field         | Description                                              |
| ------------- | -------------------------------------------------------- |
| `metric_name` | Unique name of the metric (used as DuckDB table name)    |
| `description` | Short explanation of the metric purpose                  |
| `owner`       | Email or team responsible                                |
| `schedule`    | Optional cron-like string for scheduling (metadata only) |
| `sql`         | Valid SQL statement returning a dataset                  |

Example:

metric_name: total_orders_last_10_days

description: Total number of orders placed by each customer in the last 10 days

owner: data.analyst@shopback.com

schedule: "0 8 * * *" # Metadata only - not implemented in prototype

sql: |

  SELECT
    c.customer_id,
    COUNT(o.order_id) AS total_orders_last_10_days
  FROM orders o
  JOIN customers c ON o.customer_id = c.customer_id
  WHERE o.order_purchase_timestamp >= DATE '2023-09-01'
  GROUP BY 1
  
# Step 3: Validate it to ensures your YAML and SQL syntax are valid.
python src/validate_yaml.py


## 🧠 Notes for running and testing Metrics
-All .yaml metrics are independent, meaning each metric can be executed separately without depending on others.
-However, the provided scripts are designed to run all metrics by default for convenience.














