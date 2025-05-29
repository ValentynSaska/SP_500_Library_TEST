import os
import asyncio
import yfinance as yf
import pandas as pd
import psycopg2
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def fetch_sp500_symbols():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    df = pd.read_html(url)[0]
    # Корекція символів типу BRK.B -> BRK-B
    symbols = [s.replace('.', '-') for s in df["Symbol"].tolist()]
    return symbols

def execute_ddl_from_file(cursor, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        ddl_sql = f.read()
        cursor.execute(ddl_sql)

def fetch_yf_data_sync(symbol, tf):
    period_map = {
        "daily": ("2y", "1d"),
        "hourly": ("20d", "1h"),
        "5min": ("2d", "5m")
    }
    period, interval = period_map[tf]
    ticker = yf.Ticker(symbol)
    try:
        df = ticker.history(period=period, interval=interval)
        df = df.reset_index()
        return df
    except Exception as e:
        print(f"Помилка при завантаженні {symbol}: {e}")
        return pd.DataFrame()

async def fetch_yf_data(symbol, tf):
    return await asyncio.to_thread(fetch_yf_data_sync, symbol, tf)

def store_to_db(symbol, tf, df, cursor):
    table_map = {
        "daily": "sp500_daily",
        "hourly": "sp500_hourly",
        "5min": "sp500_minute"
    }
    table = table_map[tf]

    for _, row in df.iterrows():
        ts = row.get("Datetime") or row.get("Date")
        if pd.notna(ts):
            ts = ts.tz_localize(None) if hasattr(ts, 'tz_localize') else ts
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            try:
                cursor.execute(
                    f"""
                    INSERT INTO sp_500_2.{table} (symbol, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (symbol, ts, row["Open"], row["High"], row["Low"], row["Close"], int(row["Volume"]))
                )
            except Exception as e:
                print(f"    INSERT помилка: {e} для {symbol} @ {ts}")

async def process_tf(tf: str, symbols):
    intervals_sec = {
        "daily": 86400,    # 1 день
        "hourly": 3600,    # 1 година
        "5min": 300        # 5 хв
    }

    while True:
        print(f"\n  Завантаження {tf} почалось: {datetime.now(timezone.utc).isoformat()}")
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cursor = conn.cursor()

            for symbol in tqdm_asyncio(symbols, desc=f"Processing {tf}"):
                df = await fetch_yf_data(symbol, tf)
                if not df.empty:
                    store_to_db(symbol, tf, df, cursor)
                    conn.commit()

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Помилка при роботі з БД або даними для {tf}: {e}")

        print(f"  Завантаження {tf} завершено: {datetime.now(timezone.utc).isoformat()}")
        await asyncio.sleep(intervals_sec[tf])

async def prepare_db():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    execute_ddl_from_file(cursor, os.path.join(os.path.dirname(__file__), "ddl.sql"))
    conn.commit()
    cursor.close()
    conn.close()

async def main():
    await prepare_db()
    symbols = fetch_sp500_symbols()

    await asyncio.gather(
        process_tf("daily", symbols),
        process_tf("hourly", symbols),
        process_tf("5min", symbols),
    )

if __name__ == "__main__":
    asyncio.run(main())
