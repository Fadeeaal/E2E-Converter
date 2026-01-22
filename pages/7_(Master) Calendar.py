import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Calendar Loader", layout="wide")
st.title("Calendar Loader")

# =========================
# DB CONNECTION
# =========================
@st.cache_resource
def get_engine():
    p = st.secrets["postgres"]
    url = (
        f"postgresql+psycopg2://{p['user']}:{p['password']}"
        f"@{p['host']}:{p['port']}/{p['database']}"
        f"?sslmode=require"
    )
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

TABLE_NAME = "calendar_cs"

# =========================
# HELPERS
# =========================
def ensure_table():
    sql = text(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
      cal_date DATE PRIMARY KEY,
      cal_week INTEGER NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)
    with engine.begin() as conn:
        conn.execute(sql)

def fetch_preview(limit=50):
    with engine.connect() as conn:
        return pd.read_sql(
            text(f"SELECT cal_date, cal_week FROM {TABLE_NAME} ORDER BY cal_date ASC LIMIT :lim"),
            conn,
            params={"lim": limit},
        )

def count_rows():
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}")).scalar()

def truncate_table():
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TABLE_NAME}"))

def load_excel(uploaded_file) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file, sheet_name="Sheet1", engine="openpyxl")

    # Validate columns
    needed = {"Date", "Week"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in Excel: {sorted(list(missing))}")

    # Keep only Date + Week (ignore Day)
    df = df[["Date", "Week"]].copy()

    # Convert types
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["Week"] = pd.to_numeric(df["Week"], errors="coerce").astype("Int64")

    # Drop invalid
    df = df[df["Date"].notna() & df["Week"].notna()].copy()

    # Rename to DB columns
    df = df.rename(columns={"Date": "cal_date", "Week": "cal_week"})

    # Remove duplicates inside file (keep last)
    df = df.drop_duplicates(subset=["cal_date"], keep="last")

    return df

def upsert_calendar(df: pd.DataFrame):
    upsert_sql = text(f"""
    INSERT INTO {TABLE_NAME} (cal_date, cal_week, updated_at)
    VALUES (:cal_date, :cal_week, NOW())
    ON CONFLICT (cal_date)
    DO UPDATE SET
      cal_week = EXCLUDED.cal_week,
      updated_at = NOW();
    """)
    with engine.begin() as conn:
        conn.execute(upsert_sql, df.to_dict(orient="records"))

# =========================
# INIT TABLE
# =========================
ensure_table()

st.sidebar.subheader("⚠️ Danger Zone")

confirm = st.sidebar.checkbox("Yes, I want to delete all data (TRUNCATE).")
if st.sidebar.button("TRUNCATE (clear all data)") and confirm:
    truncate_table()
    st.sidebar.success("Table cleared.")
    st.rerun()

# =========================
# MAIN
# =========================

st.subheader("Preview Data in DB")
st.dataframe(fetch_preview(50), use_container_width=True)

st.markdown("---")

st.subheader("Bulk Upload Calendar Excel")
uploaded = st.file_uploader("Upload Calendar (.xlsx)", type=["xlsx"])

if uploaded:
    try:
        df_up = load_excel(uploaded)
        st.success(f"File loaded: {len(df_up):,} valid rows (Date+Week).")
        st.dataframe(df_up.head(30), use_container_width=True)

        if st.button("⬆️ Upload to DB (Upsert)"):
            with st.spinner("Uploading..."):
                upsert_calendar(df_up)
            st.success("Upload complete.")
            st.rerun()

    except Exception as e:
        st.error(f"Failed to process file: {e}")
else:
    st.caption("Upload your file first.")
