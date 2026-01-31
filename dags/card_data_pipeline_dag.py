from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1
}

DATABRICKS_CLUSTER_ID = "YOUR_CLUSTER_ID_HERE"

with DAG(
    dag_id="card_enterprise_databricks_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["databricks", "medallion"]
) as dag:

    bronze_ingestion = DatabricksSubmitRunOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
        notebook_task={
            "notebook_path": "/Workspace/Users/rajeshtavva123@gmail.com/card-enterprise-pipeline/src/bronze_ingestion/ingest_s3_to_bronze"
        }
    )

    silver_incremental = DatabricksSubmitRunOperator(
        task_id="silver_incremental_merge",
        databricks_conn_id="databricks_default",
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
        notebook_task={
            "notebook_path": "/Workspace/Users/rajeshtavva123@gmail.com/card-enterprise-pipeline/src/silver_transformation/silver_incremental_merge"
        }
    )

    gold_aggregations = DatabricksSubmitRunOperator(
        task_id="gold_aggregations",
        databricks_conn_id="databricks_default",
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
        notebook_task={
            "notebook_path": "/Workspace/Users/rajeshtavva123@gmail.com/card-enterprise-pipeline/src/gold_aggregations/gold_metrics"
        }
    )

    dq_checks = DatabricksSubmitRunOperator(
        task_id="data_quality_checks",
        databricks_conn_id="databricks_default",
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
        notebook_task={
            "notebook_path": "/Workspace/Users/rajeshtavva123@gmail.com/card-enterprise-pipeline/src/data_quality_checks/dq_validations"
        }
    )

    bronze_ingestion >> silver_incremental >> gold_aggregations >> dq_checks
