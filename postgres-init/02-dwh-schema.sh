#!/bin/bash
set -e

export PGPASSWORD="$POSTGRES_PASSWORD"

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d dwh <<'SQL'
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS dds.users (
    user_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.products (
    product_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.user_sessions (
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    session_duration_sec INTEGER NOT NULL,
    pages_visited_count INTEGER NOT NULL,
    actions_count INTEGER NOT NULL,
    device_type TEXT NOT NULL,
    pages_visited JSONB NOT NULL,
    actions JSONB NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, start_time)
) PARTITION BY RANGE (start_time);

CREATE TABLE IF NOT EXISTS dds.user_sessions_2024_01 PARTITION OF dds.user_sessions
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS dds.user_sessions_2024_02 PARTITION OF dds.user_sessions
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS dds.user_sessions_2024_03 PARTITION OF dds.user_sessions
FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS dds.user_sessions_2024_04 PARTITION OF dds.user_sessions
FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

CREATE TABLE IF NOT EXISTS dds.event_logs (
    event_id TEXT NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    page_url TEXT,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE TABLE IF NOT EXISTS dds.event_logs_2024_01 PARTITION OF dds.event_logs
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS dds.event_logs_2024_02 PARTITION OF dds.event_logs
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS dds.event_logs_2024_03 PARTITION OF dds.event_logs
FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS dds.event_logs_2024_04 PARTITION OF dds.event_logs
FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

CREATE TABLE IF NOT EXISTS dds.support_tickets (
    ticket_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    messages_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    resolution_hours NUMERIC(10, 2) NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticket_id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE IF NOT EXISTS dds.support_tickets_2024_01 PARTITION OF dds.support_tickets
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS dds.support_tickets_2024_02 PARTITION OF dds.support_tickets
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS dds.support_tickets_2024_03 PARTITION OF dds.support_tickets
FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS dds.support_tickets_2024_04 PARTITION OF dds.support_tickets
FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

CREATE TABLE IF NOT EXISTS mart.user_activity_daily (
    activity_date DATE NOT NULL,
    user_id TEXT NOT NULL,
    sessions_cnt INTEGER NOT NULL,
    total_session_duration_sec INTEGER NOT NULL,
    avg_session_duration_sec NUMERIC(12, 2) NOT NULL,
    pages_visited_total INTEGER NOT NULL,
    actions_total INTEGER NOT NULL,
    PRIMARY KEY (activity_date, user_id)
);

CREATE TABLE IF NOT EXISTS mart.support_efficiency_daily (
    report_date DATE NOT NULL,
    issue_type TEXT NOT NULL,
    status TEXT NOT NULL,
    tickets_cnt INTEGER NOT NULL,
    avg_resolution_hours NUMERIC(12, 2) NOT NULL,
    open_tickets_cnt INTEGER NOT NULL,
    PRIMARY KEY (report_date, issue_type, status)
);

CREATE INDEX IF NOT EXISTS idx_users_email ON dds.users (email);
CREATE INDEX IF NOT EXISTS idx_products_category ON dds.products (category);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON dds.user_sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_event_logs_event_type ON dds.event_logs (event_type);
CREATE INDEX IF NOT EXISTS idx_support_tickets_status ON dds.support_tickets (status);
SQL