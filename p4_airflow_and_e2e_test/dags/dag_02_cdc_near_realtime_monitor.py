#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apache Airflow DAG 2: dag_02_cdc_near_realtime_monitor
Giám sát Debezium CDC Connectors & Kích hoạt Worker Near Real-time C360 mỗi phút.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    class DAG:
        def __init__(self, dag_id, default_args=None, schedule=None, catchup=False, **kwargs):
            self.dag_id = dag_id
            self.default_args = default_args
            self.schedule = schedule
            self.catchup = catchup

    class PythonOperator:
        def __init__(self, task_id, python_callable, dag=None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.dag = dag

        def __rshift__(self, other):
            return other

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.debezium.register_connectors import build_connectors_payload
from p2_c360_standardization.c360_nrt_worker import run_c360_nrt_batch

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 11, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

dag = DAG(
    dag_id="dag_02_cdc_near_realtime_monitor",
    default_args=default_args,
    description="Lập lịch kích hoạt Worker Near Real-time mỗi phút",
    schedule="* * * * *",  # Chạy mỗi phút
    catchup=False,
    max_active_runs=1,
)


def check_cdc_status():
    """Kiểm tra cấu hình CDC Connectors."""
    payloads = build_connectors_payload()
    return {"status": "CONFIG_READY", "source": payloads["source"]["name"], "sink": payloads["sink"]["name"]}


task_check_cdc = PythonOperator(
    task_id="task_check_connectors",
    python_callable=check_cdc_status,
    dag=dag,
)

task_nrt_worker = PythonOperator(
    task_id="task_run_nrt_worker",
    python_callable=run_c360_nrt_batch,
    dag=dag,
)

task_check_cdc >> task_nrt_worker
