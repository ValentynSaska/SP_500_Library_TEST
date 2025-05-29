CREATE SCHEMA IF NOT EXISTS sp_500_2;

CREATE TABLE IF NOT EXISTS sp_500_2.sp500_daily (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    timestamp TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT
);

CREATE TABLE IF NOT EXISTS sp_500_2.sp500_hourly (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    timestamp TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT
);

CREATE TABLE IF NOT EXISTS sp_500_2.sp500_minute (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    timestamp TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT
);
