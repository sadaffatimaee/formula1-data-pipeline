"""
OpenF1 Real-Time Loader

Loads real-time Race & Qualifying session data into:
- RAW.OPENF1_SESSIONS_REALTIME
- RAW.OPENF1_LAPS_REALTIME
- RAW.OPENF1_INTERVALS_REALTIME
- RAW.OPENF1_POSITION_REALTIME
- RAW.OPENF1_RACE_CONTROL_REALTIME
"""

import json
import math
from datetime import datetime, timezone

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.errors import ProgrammingError


# ===================== CONFIG =====================

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
BASE = "https://api.openf1.org/v1"

# Use a real season year
YEAR = 2023

ENDPOINTS = ["laps", "intervals", "position", "race_control"]
SESSIONS_FILTER = ["Race", "Qualifying"]

BATCH_SIZE = 5000
SCHEMA_NAME = "RAW"
SESSIONS_TABLE_REALTIME = f"{SCHEMA_NAME}.OPENF1_SESSIONS_REALTIME"


# ===================== UTILITIES =====================

def fetch_session_endpoint(endpoint, session_key):
    params = {"session_key": session_key}
    try:
        r = requests.get(f"{BASE}/{endpoint}", params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        print(f"fetch_session_endpoint {endpoint}: HTTP {r.status_code}")
    except Exception as e:
        print(f"fetch error {endpoint}: {e}")
    return []


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    null_equivalents = {"", " ", "None", "none", "NULL", "null", "NaN", "nan", "NAN"}

    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None if isinstance(x, str) and x.strip() in null_equivalents else x
        )

    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )

    df = df.astype(object).where(pd.notnull(df), None)

    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    return df


def normalize_intervals_df(df: pd.DataFrame) -> pd.DataFrame:
    def to_int_or_none(x):
        try:
            return None if x is None else str(int(float(str(x).strip())))
        except Exception:
            return None

    def parse_lap_gap(x):
        try:
            s = str(x).strip().upper()
            if "LAP" in s:
                parts = s.replace("+", "").split()
                for p in parts:
                    try:
                        return str(int(float(p)))
                    except Exception:
                        continue
                return None
            return str(float(s))
        except Exception:
            return None

    if "driver_number" in df.columns:
        df["driver_number"] = df["driver_number"].apply(to_int_or_none)

    if "gap_to_leader" in df.columns:
        df["gap_to_leader"] = df["gap_to_leader"].apply(parse_lap_gap)

    return df


def execute_append_logic(cursor, table_name, df):
    cols = ", ".join([f'"{c}" VARCHAR' for c in df.columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})")

    data = df.values.tolist()
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        cursor.executemany(insert_sql, batch)
        print(f"Inserted batch of {len(batch)} rows into {table_name}")


def load_to_snowflake(df, table_name):
    if df.empty:
        print(f"No rows for {table_name}")
        return

    df = clean_df(df)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN")
        execute_append_logic(cursor, table_name, df)
        conn.commit()
    except ProgrammingError as e:
        conn.rollback()
        msg = str(e)
        print(f"ProgrammingError loading {table_name}: {msg}")

        if ("002020" in msg) or ("match column list" in msg) or ("Numeric value" in msg):
            print("Dropping & recreating table for schema mismatch…")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            execute_append_logic(cursor, table_name, df)
            conn.commit()
        else:
            print("Unrecoverable error:", msg)
    finally:
        cursor.close()
        conn.close()


def delete_session_rows(table_name, session_key):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN")
        cursor.execute(
            f'DELETE FROM {table_name} WHERE "session_key" = %s',
            (session_key,),
        )
        conn.commit()
    except ProgrammingError:
        conn.rollback()
        print(f"Table {table_name} does not exist yet — skipping delete.")
    finally:
        cursor.close()
        conn.close()


# ===================== MAIN TASK =====================

def run_realtime_sim(**context):
    try:
        sessions = requests.get(f"{BASE}/sessions?year={YEAR}", timeout=20).json()
    except Exception as e:
        print(f"Failed fetching sessions: {e}")
        return

    df = pd.DataFrame(sessions)
    if df.empty:
        print("No sessions returned.")
        return

    time_col = "date_end" if "date_end" in df.columns else "date_start"
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    # SAFE handling of meeting_name (fix for AttributeError)
    if "meeting_name" not in df.columns:
        df["meeting_name"] = "Unknown GP"
    else:
        df["meeting_name"] = df["meeting_name"].fillna("Unknown GP")

    session_name_col = "session_name" if "session_name" in df.columns else "session_type"
    df[session_name_col] = df[session_name_col].fillna("Unknown")

    now_utc = datetime.now(timezone.utc)

    filtered = df[
        (df[time_col] <= now_utc) & (df[session_name_col].isin(SESSIONS_FILTER))
    ].copy()
    if filtered.empty:
        print("No completed Race/Qualifying.")
        return

    filtered["type_priority"] = filtered[session_name_col].apply(
        lambda x: 1 if x == "Race" else 0
    )

    target = filtered.sort_values(
        by=["type_priority", time_col],
        ascending=[False, False],
    ).iloc[0]

    sk = target["session_key"]
    print(f"Loading realtime session_key={sk}")

    # Store realtime session row
    delete_session_rows(SESSIONS_TABLE_REALTIME, sk)
    load_to_snowflake(target.to_frame().T, SESSIONS_TABLE_REALTIME)

    # Load endpoints
    for ep in ENDPOINTS:
        tbl = f"{SCHEMA_NAME}.OPENF1_{ep.upper()}_REALTIME"
        data = fetch_session_endpoint(ep, sk)

        if not data:
            print(f"No rows for {ep}")
            continue

        df_new = pd.DataFrame(data)

        if "session_key" not in df_new.columns:
            df_new["session_key"] = sk
        if "meeting_key" not in df_new.columns and "meeting_key" in target:
            df_new["meeting_key"] = target["meeting_key"]
        if "year" not in df_new.columns:
            df_new["year"] = YEAR

        if ep == "intervals":
            df_new = normalize_intervals_df(df_new)

        delete_session_rows(tbl, sk)
        load_to_snowflake(df_new, tbl)


# ===================== DAG BLOCK =====================

with DAG(
    dag_id="openf1_realtime_nov",
    default_args={"owner": "airflow"},
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as realtime_dag:

    t1 = PythonOperator(
        task_id="run_realtime",
        python_callable=run_realtime_sim,
    )
