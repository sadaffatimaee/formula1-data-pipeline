"""
OpenF1 Historical Loader (Recent 3–4 Months)

Loads historical Race + Qualifying data into:
- RAW.OPENF1_SESSIONS_HISTORICAL
- RAW.OPENF1_LAPS_HISTORICAL
- RAW.OPENF1_INTERVALS_HISTORICAL
- RAW.OPENF1_POSITION_HISTORICAL
- RAW.OPENF1_RACE_CONTROL_HISTORICAL
"""

import json
import math
from datetime import datetime, timezone, timedelta

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
WINDOW_DAYS = 120   # 3–4 months

ENDPOINTS = ["laps", "intervals", "position", "race_control"]
SESSIONS_FILTER = ["Race", "Qualifying"]

BATCH_SIZE = 5000
SCHEMA_NAME = "RAW"

SESSIONS_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_SESSIONS_HISTORICAL"
LAPS_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_LAPS_HISTORICAL"
INTERVALS_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_INTERVALS_HISTORICAL"
POSITION_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_POSITION_HISTORICAL"
RACE_CTRL_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_RACE_CONTROL_HISTORICAL"


# ===================== UTILITIES =====================

def fetch_session_endpoint(endpoint, session_key):
    params = {"session_key": session_key}
    try:
        r = requests.get(f"{BASE}/{endpoint}", params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        print(f"fetch_session_endpoint {endpoint}: HTTP {r.status_code}")
    except Exception as e:
        print(f"fetch error for {endpoint}: {e}")
    return []


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    null_equivalents = {"", " ", "None", "none", "NULL", "null", "NaN", "nan", "NAN"}

    # Null-like → None
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None if isinstance(x, str) and x.strip() in null_equivalents else x
        )
    # JSON encode lists/dicts
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

    df = df.astype(object).where(pd.notnull(df), None)

    # Cast to string except None
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
        print(f"Empty dataframe, skipping {table_name}")
        return

    df = clean_df(df)
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN")
        execute_append_logic(cursor, table_name, df)
        conn.commit()
        print(f"Loaded {len(df)} rows into {table_name}")

    except ProgrammingError as e:
        conn.rollback()
        msg = str(e)
        print(f"ProgrammingError loading {table_name}: {msg}")

        if ("002020" in msg) or ("match column list" in msg) or ("Numeric value" in msg):
            print("Schema mismatch — dropping and recreating table")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            execute_append_logic(cursor, table_name, df)
            conn.commit()
        else:
            print("Non-recoverable error.")

    finally:
        cursor.close()
        conn.close()


def get_loaded_session_keys() -> set:
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    try:
        df = hook.get_pandas_df(
            f'SELECT DISTINCT "session_key" FROM {SESSIONS_TABLE_HIST}'
        )
        return set(df["session_key"].tolist()) if not df.empty else set()
    except Exception as e:
        print(f"Error getting loaded session_keys: {e}")
        return set()


# ===================== MAIN TASK =====================

def run_historical_recent(**context):
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=WINDOW_DAYS)
    print(f"Window: {cutoff} → {now_utc}")

    # Fetch all sessions for the year
    try:
        sessions = requests.get(f"{BASE}/sessions?year={YEAR}", timeout=30).json()
    except Exception as e:
        print("Failed to fetch sessions:", e)
        return

    df = pd.DataFrame(sessions)
    if df.empty:
        print("No sessions returned")
        return

    time_col = "date_end" if "date_end" in df.columns else "date_start"
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    # Safe handling of meeting_name
    if "meeting_name" not in df.columns:
        df["meeting_name"] = "Unknown GP"
    else:
        df["meeting_name"] = df["meeting_name"].fillna("Unknown GP")

    session_name_col = "session_name" if "session_name" in df.columns else "session_type"
    df[session_name_col] = df[session_name_col].fillna("Unknown")

    # Identify latest Race/Qualifying (for realtime DAG)
    completed = df[(df[time_col] <= now_utc) & (df[session_name_col].isin(SESSIONS_FILTER))].copy()

    latest_sk = None
    if not completed.empty:
        completed["type_priority"] = completed[session_name_col].apply(
            lambda x: 1 if x == "Race" else 0
        )
        latest_sk = completed.sort_values(
            by=["type_priority", time_col],
            ascending=[False, False]
        ).iloc[0]["session_key"]

    mask_window = (df[time_col] >= cutoff) & (df[time_col] <= now_utc)
    mask_type = df[session_name_col].isin(SESSIONS_FILTER)

    filtered = df[mask_window & mask_type].copy()
    if latest_sk:
        filtered = filtered[filtered["session_key"] != latest_sk]

    if filtered.empty:
        print("No sessions in window")
        return

    loaded = get_loaded_session_keys()

    for _, row in filtered.sort_values(by=[time_col]).iterrows():
        sk = row["session_key"]
        if sk in loaded:
            print(f"Session_key={sk} already loaded; skipping.")
            continue

        meeting_name = row["meeting_name"]
        session_name = row[session_name_col]
        print(f"Loading: {meeting_name} - {session_name} ({sk})")

        # Store session row
        load_to_snowflake(row.to_frame().T, SESSIONS_TABLE_HIST)

        # Load endpoints
        for ep in ENDPOINTS:
            if ep == "laps":
                tbl = LAPS_TABLE_HIST
            elif ep == "intervals":
                tbl = INTERVALS_TABLE_HIST
            elif ep == "position":
                tbl = POSITION_TABLE_HIST
            elif ep == "race_control":
                tbl = RACE_CTRL_TABLE_HIST
            else:
                continue

            data = fetch_session_endpoint(ep, sk)
            if not data:
                print(f"No {ep} data for {sk}")
                continue

            df_new = pd.DataFrame(data)
            if "session_key" not in df_new.columns:
                df_new["session_key"] = sk
            if "meeting_key" not in df_new.columns and "meeting_key" in row:
                df_new["meeting_key"] = row["meeting_key"]
            if "year" not in df_new.columns:
                df_new["year"] = YEAR

            if ep == "intervals":
                df_new = normalize_intervals_df(df_new)

            load_to_snowflake(df_new, tbl)

        loaded.add(sk)


# ===================== DAG BLOCK =====================

with DAG(
    dag_id="openf1_historical_recent",
    default_args={"owner": "airflow"},
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as hist_dag:

    t_hist = PythonOperator(
        task_id="run_historical_recent",
        python_callable=run_historical_recent,
    )
