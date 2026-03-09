from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values

from app.data_generator import generate_documents
from app.db import get_mongo_db, pg_conn


def seed_mongo():
    db = get_mongo_db()
    if db.user_sessions.count_documents({}) > 0:
        return

    users, products, sessions, events, tickets = generate_documents()

    db.users.insert_many(users)
    db.products.insert_many(products)
    db.user_sessions.insert_many(sessions)
    db.event_logs.insert_many(events)
    db.support_tickets.insert_many(tickets)


def replicate_users():
    db = get_mongo_db()
    rows = []
    seen = set()

    for doc in db.users.find({}, {"_id": 0}):
        key = doc["user_id"]
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            (
                doc["user_id"],
                doc["name"],
                doc["email"],
                doc["created_at"],
            )
        )

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO dds.users (user_id, name, email, created_at)
                VALUES %s
                ON CONFLICT (user_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    created_at = EXCLUDED.created_at;
                """,
                rows,
            )
        conn.commit()


def replicate_products():
    db = get_mongo_db()
    rows = []
    seen = set()

    for doc in db.products.find({}, {"_id": 0}):
        key = doc["product_id"]
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            (
                doc["product_id"],
                doc["name"],
                doc["category"],
                doc["price"],
                doc["created_at"],
            )
        )

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO dds.products (product_id, name, category, price, created_at)
                VALUES %s
                ON CONFLICT (product_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price,
                    created_at = EXCLUDED.created_at;
                """,
                rows,
            )
        conn.commit()


def replicate_sessions():
    db = get_mongo_db()
    rows = []
    seen = set()

    for doc in db.user_sessions.find({}, {"_id": 0}):
        key = (doc["session_id"], doc["start_time"])
        if key in seen:
            continue
        seen.add(key)

        duration = int((doc["end_time"] - doc["start_time"]).total_seconds())

        rows.append(
            (
                doc["session_id"],
                doc["user_id"],
                doc["start_time"],
                doc["end_time"],
                duration,
                len(doc.get("pages_visited", [])),
                len(doc.get("actions", [])),
                doc.get("device", {}).get("type", "unknown"),
                json.dumps(doc.get("pages_visited", []), ensure_ascii=False),
                json.dumps(doc.get("actions", []), ensure_ascii=False),
            )
        )

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO dds.user_sessions
                (
                    session_id,
                    user_id,
                    start_time,
                    end_time,
                    session_duration_sec,
                    pages_visited_count,
                    actions_count,
                    device_type,
                    pages_visited,
                    actions
                )
                VALUES %s
                ON CONFLICT (session_id, start_time) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    end_time = EXCLUDED.end_time,
                    session_duration_sec = EXCLUDED.session_duration_sec,
                    pages_visited_count = EXCLUDED.pages_visited_count,
                    actions_count = EXCLUDED.actions_count,
                    device_type = EXCLUDED.device_type,
                    pages_visited = EXCLUDED.pages_visited,
                    actions = EXCLUDED.actions,
                    source_loaded_at = NOW();
                """,
                rows,
            )
        conn.commit()


def replicate_events():
    db = get_mongo_db()
    rows = []
    seen = set()

    for doc in db.event_logs.find({}, {"_id": 0}):
        key = (doc["event_id"], doc["timestamp"])
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            (
                doc["event_id"],
                doc["timestamp"],
                doc.get("event_type", "unknown"),
                doc.get("details", {}).get("page"),
            )
        )

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO dds.event_logs (event_id, event_ts, event_type, page_url)
                VALUES %s
                ON CONFLICT (event_id, event_ts) DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    page_url = EXCLUDED.page_url,
                    source_loaded_at = NOW();
                """,
                rows,
            )
        conn.commit()


def replicate_tickets():
    db = get_mongo_db()
    rows = []
    seen = set()

    for doc in db.support_tickets.find({}, {"_id": 0}):
        key = (doc["ticket_id"], doc["created_at"])
        if key in seen:
            continue
        seen.add(key)

        resolution_hours = round(
            (doc["updated_at"] - doc["created_at"]).total_seconds() / 3600,
            2,
        )

        rows.append(
            (
                doc["ticket_id"],
                doc["user_id"],
                doc["status"],
                doc["issue_type"],
                len(doc.get("messages", [])),
                doc["created_at"],
                doc["updated_at"],
                resolution_hours,
            )
        )

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO dds.support_tickets
                (
                    ticket_id,
                    user_id,
                    status,
                    issue_type,
                    messages_count,
                    created_at,
                    updated_at,
                    resolution_hours
                )
                VALUES %s
                ON CONFLICT (ticket_id, created_at) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    status = EXCLUDED.status,
                    issue_type = EXCLUDED.issue_type,
                    messages_count = EXCLUDED.messages_count,
                    updated_at = EXCLUDED.updated_at,
                    resolution_hours = EXCLUDED.resolution_hours,
                    source_loaded_at = NOW();
                """,
                rows,
            )
        conn.commit()


with DAG(
    dag_id="mongo_to_postgres_replication",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "replication"],
) as dag:
    t1 = PythonOperator(task_id="seed_mongo", python_callable=seed_mongo)
    t2 = PythonOperator(task_id="replicate_users", python_callable=replicate_users)
    t3 = PythonOperator(task_id="replicate_products", python_callable=replicate_products)
    t4 = PythonOperator(task_id="replicate_sessions", python_callable=replicate_sessions)
    t5 = PythonOperator(task_id="replicate_events", python_callable=replicate_events)
    t6 = PythonOperator(task_id="replicate_tickets", python_callable=replicate_tickets)

    t1 >> [t2, t3, t4, t5, t6]