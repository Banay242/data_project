create schema if not exists postgres;

\connect postgres;

create table if not exists ods.products
(
    product_id    serial
        primary key,
    bank_name     varchar(100),
    product_type  varchar(100),
    interest_rate numeric(5, 2)
);

create table if not exists ods.credit_leads
(
    id         serial
        primary key,
    user_id    integer not null,
    bank_name  varchar(50),
    amount     numeric(10, 2),
    status     varchar(20),
    updated_at timestamp default now()
);

create table if not exists ods.applications
(
    app_id           uuid not null
        primary key,
    product_id       integer,
    user_id          integer,
    requested_amount numeric(15, 2),
    status           varchar(20),
    created_date     date,
    metadata         jsonb
);

create table if not exists ods.applications2
(
    app_id           uuid not null
        primary key,
    product_id       integer,
    user_id          integer,
    requested_amount numeric(15, 2),
    status           varchar(20),
    created_date     date,
    metadata         jsonb
);

ALTER TABLE ods.credit_leads REPLICA IDENTITY FULL;

-- Таблица для Heartbeats
CREATE TABLE IF NOT EXISTS ods.dbz_heartbeat (
    id integer PRIMARY KEY,
    ts timestamp
);

-- Таблица для Signals
CREATE TABLE IF NOT EXISTS ods.dbz_signal (
    id varchar(64) PRIMARY KEY,
    type varchar(32),
    data varchar(2048)
);