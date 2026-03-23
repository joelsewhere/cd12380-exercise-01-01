# ============================================================
#  SOLUTION: @task Decorators → PythonOperator
# ============================================================
#  The same ETL pipeline from decorator_dag.py is rewritten
#  using traditional operator syntax.
# ============================================================


# ── STEP 1 ───────────────────────────────────────────────────
# Import the DAG and PythonOperator objects from the airflow
# package.  You will also need `datetime` from the standard
# library to set the `start_date` when you configure the DAG.
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# ── STEP 2 ───────────────────────────────────────────────────
# Define plain Python functions for each pipeline stage.

# 2a) extract() — returns the raw_data dictionary
def extract():
    raw_data = {"temperature": 72.5, "humidity": 45, "city": "Chicago"}
    print(f"Extracted: {raw_data}")
    return raw_data


# 2b) transform(**kwargs) — pulls raw_data from XCom,
#     converts temperature to Celsius, uppercases the city,
#     and returns the cleaned dictionary
def transform(**kwargs):
    ti = kwargs["ti"]
    raw_data = ti.xcom_pull(task_ids="extract")
    transformed = {
        "temp_celsius": round((raw_data["temperature"] - 32) * 5 / 9, 2),
        "humidity": raw_data["humidity"],
        "city": raw_data["city"].upper(),
    }
    print(f"Transformed: {transformed}")
    return transformed


# 2c) load(**kwargs) — pulls transformed_data from XCom
#     and prints the final record
def load(**kwargs):
    ti = kwargs["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform")
    print(f"Loading record to database: {transformed_data}")
    print("Load complete.")


# ── STEP 3 ───────────────────────────────────────────────────
# Instantiate a DAG object using a `with` context manager.
with DAG(
    dag_id="traditional_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial"],
) as dag:

    # ── STEP 4 ─────────────────────────────────────────────
    # Create a PythonOperator for each pipeline stage.

    # 4a) extract_task
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    # 4b) transform_task
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    # 4c) load_task
    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # ── STEP 5 ─────────────────────────────────────────────
    # Set task dependencies using the bitshift operators.
    extract_task >> transform_task >> load_task