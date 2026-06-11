from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner':            'dharun',
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

dag = DAG(
    'stock_pipeline',
    default_args=default_args,
    description='Stock market streaming pipeline orchestration',
    schedule_interval='0 * * * *',   # every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stock', 'streaming', 'lakehouse']
)

def check_bronze_health(**context):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("HealthCheck").getOrCreate()
    df = spark.read.format("delta").load("/tmp/delta/bronze/trades")
    count = df.count()
    print(f"Bronze layer row count: {count}")
    if count == 0:
        raise ValueError("Bronze layer is empty — pipeline may be down")
    return count

def run_gold_build(**context):
    import subprocess
    result = subprocess.run(
        ["python", "/opt/airflow/dags/../../../delta/lakehouse.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Gold build failed: {result.stderr}")

def validate_data_quality(**context):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("DQ").getOrCreate()
    silver = spark.read.format("delta").load("/tmp/delta/silver/vwap_1min")

    null_count  = silver.filter(silver.vwap.isNull()).count()
    neg_vol     = silver.filter(silver.total_volume < 0).count()
    symbol_check = silver.select("symbol").distinct().count()

    print(f"Nulls: {null_count} | Neg volumes: {neg_vol} | Symbols: {symbol_check}")
    if null_count > 100 or neg_vol > 0:
        raise ValueError(f"Data quality check failed — nulls={null_count}, neg_vol={neg_vol}")
    return {"nulls": null_count, "symbols": symbol_check}

# ── Tasks ────────────────────────────────────────────────────
t1 = PythonOperator(
    task_id='check_bronze_health',
    python_callable=check_bronze_health,
    dag=dag
)

t2 = PythonOperator(
    task_id='validate_silver_quality',
    python_callable=validate_data_quality,
    dag=dag
)

t3 = PythonOperator(
    task_id='build_gold_layer',
    python_callable=run_gold_build,
    dag=dag
)

t4 = BashOperator(
    task_id='run_dbt_transforms',
    bash_command='cd /opt/airflow/dbt_project && dbt run --select gold',
    dag=dag
)

t5 = BashOperator(
    task_id='run_dbt_tests',
    bash_command='cd /opt/airflow/dbt_project && dbt test',
    dag=dag
)

t6 = BashOperator(
    task_id='optimize_delta_tables',
    bash_command='python /opt/airflow/../delta/lakehouse.py optimize',
    dag=dag
)

t7 = PythonOperator(
    task_id='pipeline_health_report',
    python_callable=lambda **kw: print("Pipeline completed successfully"),
    dag=dag
)

# ── DAG dependency chain ─────────────────────────────────────
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7