#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apache Airflow DAG 1: dag_01_initial_setup_full_load
Quy trình nạp toàn lượng ban đầu từ Nguồn đến C360 và Đối soát:
  task_load_danhmuc >> task_gen_seed >> task_full_load_mirror >> task_build_map_dbhc >> task_full_load_c360 >> task_initial_reconciliation
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Airflow mock / import fallback để chạy độc lập hoặc trên Airflow Worker
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    # Lớp giả lập cho môi trường chạy không có cài Apache Airflow
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

from p1_source_and_mirror.full_load_jdbc import run_full_load_all
from p1_source_and_mirror.gen_seed_nhankhau import generate_seed_dataset
from p1_source_and_mirror.load_danhmuc import run_load_danhmuc
from p2_c360_standardization.build_map_dbhc import build_and_refresh_dbhc_maps
from p2_c360_standardization.c360_etl_full_load import run_c360_full_load
from p3_reconciliation_bi_api.reconcile_3_tiers import run_3tier_reconciliation

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 11, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dag_01_initial_setup_full_load",
    default_args=default_args,
    description="Pipeline nạp dữ liệu ban đầu toàn trình (HGD -> Mirror -> C360 -> Đối soát)",
    schedule=None,  # Chạy Manual / On-demand
    catchup=False,
)

task_1_danhmuc = PythonOperator(
    task_id="task_load_danhmuc",
    python_callable=run_load_danhmuc,
    dag=dag,
)

task_2_seed = PythonOperator(
    task_id="task_gen_seed",
    python_callable=generate_seed_dataset,
    dag=dag,
)

task_3_full_load_mirror = PythonOperator(
    task_id="task_full_load_mirror",
    python_callable=run_full_load_all,
    dag=dag,
)

task_4_map_dbhc = PythonOperator(
    task_id="task_build_map_dbhc",
    python_callable=build_and_refresh_dbhc_maps,
    dag=dag,
)

task_5_full_load_c360 = PythonOperator(
    task_id="task_full_load_c360",
    python_callable=run_c360_full_load,
    dag=dag,
)

task_6_reconcile = PythonOperator(
    task_id="task_initial_reconciliation",
    python_callable=run_3tier_reconciliation,
    dag=dag,
)

task_1_danhmuc >> task_2_seed >> task_3_full_load_mirror >> task_4_map_dbhc >> task_5_full_load_c360 >> task_6_reconcile
