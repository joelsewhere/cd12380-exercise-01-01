# Airflow DAG Refactoring Exercise

In this exercise you will rewrite an Airflow DAG from the modern `@task` decorator syntax into traditional `PythonOperator` syntax.

---

## Files

| File | Description |
|---|---|
| `decorator_dag.py` | Reference DAG — read this first |
| `traditional_dag.py` | Your working file — fill in the blanks |


---

## Instructions

1. Open `decorator_dag.py` and read through the pipeline. It runs a simple weather ETL: **extract → transform → load**. Note how return values are passed directly between tasks.

2. Open `traditional_dag.py`. Work through each numbered step, replacing every `#### YOUR CODE HERE` block with real code:
   - **Step 1** — Update the imports
   - **Step 2** — Define the three callable functions outside the DAG. Pay close attention to the XCom notes — this is the most important conceptual difference from the decorator syntax.
   - **Step 3** — Instantiate the DAG with a `with` context manager
   - **Step 4** — Create a `PythonOperator` for each task
   - **Step 5** — Chain the tasks with `>>`

---

## Key Concept: XCom

The decorator syntax hides data passing between tasks — return values are forwarded automatically. With `PythonOperator` you must handle this yourself using **XCom**:

```python
# Push — just return a value from your callable
def extract():
    return {"temperature": 72.5, "city": "Chicago"}

# Pull — access `ti` from kwargs to retrieve the upstream value
def transform(**kwargs):
    ti = kwargs["ti"]
    raw_data = ti.xcom_pull(task_ids="extract")
```