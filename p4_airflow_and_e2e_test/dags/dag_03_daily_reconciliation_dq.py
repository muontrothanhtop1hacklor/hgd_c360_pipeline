#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apache Airflow DAG 3: dag_03_daily_reconciliation_dq
Lập lịch đối soát 3 tầng và cập nhật snapshot báo cáo BI hàng ngày vào 01:00 AM.
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

from p1_source_and_mirror.db_env import get_connection
from p3_reconciliation_bi_api.reconcile_3_tiers import run_3tier_reconciliation

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 11, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dag_03_daily_reconciliation_dq",
    default_args=default_args,
    description="Đối soát dữ liệu 3 tầng và cập nhật báo cáo chất lượng dữ liệu hàng ngày",
    schedule="0 1 * * *",  # 01:00 AM hàng ngày
    catchup=False,
)


def refresh_bi_views():
    """Tạo mới/cập nhật 3 Views Data Mart phục vụ BI."""
    conn = get_connection()
    cur = conn.cursor()
    views_sql_path = PROJECT_ROOT / "p3_reconciliation_bi_api" / "views_bi_datamart.sql"
    cur.executescript(views_sql_path.read_text(encoding="utf-8"))
    conn.commit()
    conn.close()
    return {"status": "VIEWS_REFRESHED"}


task_reconcile = PythonOperator(
    task_id="task_reconcile_all_tiers",
    python_callable=run_3tier_reconciliation,
    dag=dag,
)

task_views = PythonOperator(
    task_id="task_refresh_bi_views",
    python_callable=refresh_bi_views,
    dag=dag,
)

task_reconcile >> task_views
