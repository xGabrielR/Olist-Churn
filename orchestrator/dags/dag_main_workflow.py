from airflow import DAG
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from airflow.models.baseoperator import chain

from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor
from airflow.providers.amazon.aws.operators.dms import DmsStartTaskOperator

from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)

from datetime import timedelta, datetime

import workflow_emr_config as w

DEFAULT_ARGS = {
    "retries": 0,
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1)
}

@dag(
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily", # Daily UTC Time.
    dagrun_timeout=timedelta(minutes=120),
    tags=["emr", "bronze", "silver", "gold", "abt"],
    description="Workflow Processing Data Engineering & ABT",
)
def MAIN_DATA_WORKFLOW():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("ingestion"):
        dms_init_task = DmsStartTaskOperator(
            task_id="dms_init_task",
            start_replication_task_type="reload-target",
            replication_task_arn=""
        )

        dms_task_sensor = DmsTaskCompletedSensor(
            task_id="dms_sensor_task",
            replication_task_arn="",
            mode="poke",
            poke_interval=60,
            exponential_backoff=False
        )

    with TaskGroup("processing"):
        emr_cluster = EmrCreateJobFlowOperator(
            task_id="job_flow",
            job_flow_overrides=w.JOB_FLOW_OVERRIDES
        )
        
        terminate_cluster = EmrTerminateJobFlowOperator(
            task_id="terminate",
            job_flow_id=emr_cluster.output
        )

        steps = []
        for step_config in w.SPARK_ICEBERG_STEPS:
            name = step_config["Name"].replace("process_iceberg_", "")
            step = EmrAddStepsOperator(
                task_id=f"{name}_step",
                job_flow_id=emr_cluster.output,
                steps=[step_config],
                wait_for_completion=True
            )

            steps.append(step)

    chain(
        start,
        dms_init_task,
        dms_task_sensor,
        emr_cluster,
        *steps,
        terminate_cluster,
        end
    )

MAIN_DATA_WORKFLOW()