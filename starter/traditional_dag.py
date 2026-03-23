# ============================================================
#  REFACTORING EXERCISE: @task Decorators → PythonOperator
# ============================================================
#
#  The decorator syntax used in `decorator_dag.py` is a clean
#  shorthand, but every @task is really a PythonOperator under
#  the hood. In this exercise you'll rewrite the same ETL
#  pipeline using traditional operator syntax.
#
#  Reference DAG : decorator_dag.py
#  Goal          : produce an identical pipeline with the same
#                  task IDs, dependencies, and logic.
# ============================================================


# ── STEP 1 ───────────────────────────────────────────────────
# Import the DAG and PythonOperator objects from the airflow
# package.
#### YOUR CODE HERE


# ── STEP 2 ───────────────────────────────────────────────────
# Define plain Python functions for each pipeline stage.
#
# Unlike the decorator pattern, these functions live OUTSIDE
# the DAG definition.  PythonOperator will call them later via
# the `python_callable` argument.
#
# IMPORTANT — XCom & task communication
# In the decorator DAG, return values were passed between tasks
# automatically.  With PythonOperator you must push and pull
# values through XCom explicitly:
#
#   Push  →  return a value from your callable  (Airflow stores
#            it in XCom automatically when using PythonOperator)
#   Pull  →  use the `ti` (task instance) kwarg:
#            ti.xcom_pull(task_ids="<upstream_task_id>")
#
# Each function that needs to receive upstream data should
# accept `ti`` as an argument

# 2a) extract() — returns the raw_data dictionary
#### YOUR CODE HERE


# 2b) transform(ti) — pulls raw_data from XCom,
#     converts temperature to Celsius, uppercases the city,
#     and returns the cleaned dictionary
#### YOUR CODE HERE


# 2c) load(ti) — pulls transformed_data from XCom
#     and prints the final record
#### YOUR CODE HERE


# ── STEP 3 ───────────────────────────────────────────────────
# Instantiate a DAG object.
#
# Here we use a `with DAG(...) as dag:` context manager — any
# operator instantiated inside the block is registered to this
# DAG automatically, without needing an explicit `dag=` kwarg.
with DAG(dag_id="traditional_pipeline") as dag:


    # ── STEP 4 ─────────────────────────────────────────────
    # Create a PythonOperator for each pipeline stage.
    #
    # Required arguments for every operator:
    #   task_id         → a unique string label (match the
    #                     function names: "extract",
    #                     "transform", "load")
    #   python_callable → the plain function you defined above

    # 4a) extract_task
    #### YOUR CODE HERE


    # 4b) transform_task
    #### YOUR CODE HERE


    # 4c) load_task
    #### YOUR CODE HERE


    # ── STEP 5 ─────────────────────────────────────────────
    # Set task dependencies using the bitshift operators.
    #
    # The decorator DAG expressed dependencies implicitly by
    # passing return values between functions:
    #
    #   raw     = extract()          # extract runs first
    #   cleaned = transform(raw)     # transform runs second
    #   load(cleaned)                # load runs last
    #
    # Here you declare the same order explicitly:
    #
    #   upstream_task >> downstream_task
    #
    # Wire extract_task → transform_task → load_task
    #### YOUR CODE HERE