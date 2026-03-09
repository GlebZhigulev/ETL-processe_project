from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from app.db import pg_conn


def build_user_activity_mart():
    sql = """
    INSERT INTO mart.user_activity_daily (
        activity_date, user_id, sessions_cnt, total_session_duration_sec,
        avg_session_duration_sec, pages_visited_total, actions_total
    )
    SELECT
        start_time::date AS activity_date,
        user_id,
        COUNT(*) AS sessions_cnt,
        SUM(session_duration_sec) AS total_session_duration_sec,
        ROUND(AVG(session_duration_sec), 2) AS avg_session_duration_sec,
        SUM(pages_visited_count) AS pages_visited_total,
        SUM(actions_count) AS actions_total
    FROM dds.user_sessions
    GROUP BY 1, 2
    ON CONFLICT (activity_date, user_id) DO UPDATE SET
        sessions_cnt = EXCLUDED.sessions_cnt,
        total_session_duration_sec = EXCLUDED.total_session_duration_sec,
        avg_session_duration_sec = EXCLUDED.avg_session_duration_sec,
        pages_visited_total = EXCLUDED.pages_visited_total,
        actions_total = EXCLUDED.actions_total;
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def build_support_efficiency_mart():
    sql = """
    INSERT INTO mart.support_efficiency_daily (
        report_date, issue_type, status, tickets_cnt, avg_resolution_hours, open_tickets_cnt
    )
    SELECT
        created_at::date AS report_date,
        issue_type,
        status,
        COUNT(*) AS tickets_cnt,
        ROUND(AVG(resolution_hours), 2) AS avg_resolution_hours,
        SUM(CASE WHEN status <> 'closed' THEN 1 ELSE 0 END) AS open_tickets_cnt
    FROM dds.support_tickets
    GROUP BY 1, 2, 3
    ON CONFLICT (report_date, issue_type, status) DO UPDATE SET
        tickets_cnt = EXCLUDED.tickets_cnt,
        avg_resolution_hours = EXCLUDED.avg_resolution_hours,
        open_tickets_cnt = EXCLUDED.open_tickets_cnt;
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


with DAG(
    dag_id='build_analytics_marts',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['etl', 'marts'],
) as dag:
    user_activity = PythonOperator(task_id='build_user_activity_mart', python_callable=build_user_activity_mart)
    support_efficiency = PythonOperator(task_id='build_support_efficiency_mart', python_callable=build_support_efficiency_mart)

    [user_activity, support_efficiency]