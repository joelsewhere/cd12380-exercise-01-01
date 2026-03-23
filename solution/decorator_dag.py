from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="decorator_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial"],
)
def decorator_pipeline():

    @task
    def extract():
        raw_data = {"temperature": 72.5, "humidity": 45, "city": "Chicago"}
        print(f"Extracted: {raw_data}")
        return raw_data

    @task
    def transform(raw_data: dict):
        transformed = {
            "temp_celsius": round((raw_data["temperature"] - 32) * 5 / 9, 2),
            "humidity": raw_data["humidity"],
            "city": raw_data["city"].upper(),
        }
        print(f"Transformed: {transformed}")
        return transformed

    @task
    def load(transformed_data: dict):
        print(f"Loading record to database: {transformed_data}")
        print("Load complete.")

    raw = extract()
    cleaned = transform(raw)
    load(cleaned)


decorator_pipeline()