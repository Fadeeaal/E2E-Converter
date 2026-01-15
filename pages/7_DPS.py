import io
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Fulfilment Extractor", layout="wide")
st.title("Fulfilment Extractor (A:H + O:P + Release + DB Enrichment)")

# =========================
# DB (Neon) - reuse secrets.toml
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

@st.cache_resource
def load_calendar_map():
    """calendar_cs: cal_date(date) -> cal_week(int)"""
    sql = text("SELECT cal_date, cal_week FROM calendar_cs")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["cal_date"] = pd.to_datetime(df["cal_date"], errors="coerce").dt.date
    df = df.dropna(subset=["cal_date", "cal_week"])
    return dict(zip(df["cal_date"], df["cal_week"]))

@st.cache_resource
def load_zcorin_map():
    """zcorin_converter: material(str) -> enrichment columns"""
    sql = text("""
        SELECT material, country, brand, big_category, house, pack_format, machine_1
        FROM zcorin_converter
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    df["material"] = df["material"].astype(str).str.strip()
    df = df.dropna(subset=["material"])
    return df.set_index("material").to_dict(orient="index")

CAL_MAP = load_calendar_map()
ZCORIN_MAP = load_zcorin_map()

# =========================
# Helpers
# =========================
def norm(x) -> str:
    return str(x).strip().lower()

def sheet_has_line_header(excel_file, sheet_name: str, max_rows: int = 30) -> bool:
    """Scan top rows; skip sheet if no 'Line' (case-insensitive) exists."""
    preview = pd.read_excel(
        excel_file,
        sheet_name=sheet_name,
        header=None,
        nrows=max_rows,
        engine="openpyxl",
    )
    for r in range(len(preview)):
        vals = preview.iloc[r].tolist()
        vals = [v for v in vals if pd.notna(v)]
        if not vals:
            continue
        if "line" in [norm(v) for v in vals]:
            return True
    return False

def format_line_col_to_mon_yy(series: pd.Series) -> pd.Series:
    """If parseable as date -> format Mon-YY (e.g., Dec-26). Else keep."""
    parsed = pd.to_datetime(series, errors="coerce")
    mask = parsed.notna()
    out = series.copy()
    out.loc[mask] = parsed.loc[mask].dt.strftime("%b-%y")
    return out

def calc_release_time(ts):
    """
    Release time = Time_Finish + 5 days.
    If Saturday -> move to Monday (+2)
    If Sunday   -> move to Monday (+1)
    """
    if pd.isna(ts):
        return pd.NaT
    rt = ts + pd.Timedelta(days=5)
    if rt.weekday() == 5:      # Saturday
        rt += pd.Timedelta(days=2)
    elif rt.weekday() == 6:    # Sunday
        rt += pd.Timedelta(days=1)
    return rt

def detect_material_col(out: pd.DataFrame) -> str:
    """
    Detect material column inside output:
    priority: 'material' -> 'sap' -> fallback second column (B)
    """
    cols = list(out.columns)
    col_map = {norm(c): c for c in cols}

    if "material" in col_map:
        return col_map["material"]
    if "sap" in col_map:
        return col_map["sap"]

    # fallback: column B (index 1) if exists
    return cols[1] if len(cols) > 1 else cols[0]

def enrich_from_db(out: pd.DataFrame) -> pd.DataFrame:
    """Add enrichment cols from zcorin_converter by material."""
    material_col = detect_material_col(out)

    # normalize material values
    keys = out[material_col].astype(str).str.strip()

    # prepare new columns
    enrich_cols = ["country", "brand", "big_category", "house", "pack_format", "machine_1"]
    for c in enrich_cols:
        out[c] = keys.map(lambda k: ZCORIN_MAP.get(k, {}).get(c))

    return out

def round_FGH(out: pd.DataFrame) -> pd.DataFrame:
    """
    Round columns F,G,H from original excel selection A:H.
    In our selected out (A:H + O:P), FGH correspond to indices 5,6,7.
    """
    # indices 5,6,7 must exist (they do if A:H selected)
    for idx in [5, 6, 7]:
        if idx < out.shape[1]:
            col = out.columns[idx]
            out[col] = pd.to_numeric(out[col], errors="coerce").round(0)
    return out

def process_sheet(excel_file, sheet_name: str):
    if not sheet_has_line_header(excel_file, sheet_name):
        return None, "SKIP (no 'Line' header found)"

    # Row 1 is header => header=0
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=0, engine="openpyxl")

    # Need at least up to column P (16 cols)
    if df.shape[1] < 16:
        return None, "SKIP (not enough columns for A:H + O:P)"

    # Select A:H and O:P
    cols_idx = list(range(0, 8)) + list(range(14, 16))
    out = df.iloc[:, cols_idx].copy()
    out = out.dropna(how="all")

    # Format Line (col A) to Mon-YY
    line_col = out.columns[0]
    out[line_col] = format_line_col_to_mon_yy(out[line_col])

    # Round F,G,H
    out = round_FGH(out)

    # Time_Finish is last selected (P)
    time_finish_col = out.columns[-1]
    out[time_finish_col] = pd.to_datetime(out[time_finish_col], errors="coerce")

    # Release time/date + week
    release_ts = out[time_finish_col].apply(calc_release_time)
    out["Release time"] = pd.to_datetime(release_ts, errors="coerce").dt.date
    out["Release wk"] = out["Release time"].map(CAL_MAP)

    # Enrich from zcorin_converter
    out = enrich_from_db(out)

    return out, "OK"

# =========================
# UI
# =========================
uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])

# c1, c2, c3 = st.columns(3)
# with c1:
#     st.text_input("Columns taken", value="A:H and O:P", disabled=True)
# with c2:
#     st.text_input("Rounded", value="F, G, H", disabled=True)
# with c3:
#     st.text_input("Enrichment", value="country, brand, big_category, house, pack_format, machine_1", disabled=True)

if not uploaded:
    st.info("Upload file dulu untuk mulai.")
    st.stop()

if st.button("Process All Sheets"):
    with st.spinner("Processing sheets..."):
        xls = pd.ExcelFile(uploaded, engine="openpyxl")

        results = {}
        report = []

        for sh in xls.sheet_names:
            try:
                df_out, status = process_sheet(uploaded, sh)
                rows = 0 if df_out is None else len(df_out)
                report.append((sh, status, rows))
                if df_out is not None and not df_out.empty:
                    results[sh] = df_out
            except Exception as e:
                report.append((sh, f"ERROR: {e}", 0))

        rep_df = pd.DataFrame(report, columns=["Sheet", "Status", "Rows"])

        if not results:
            st.error("Tidak ada sheet yang berhasil diproses (semua ter-skip atau error).")
            st.dataframe(rep_df, use_container_width=True)
            st.stop()

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            for sh, df_out in results.items():
                df_out.to_excel(writer, sheet_name=sh, index=False)
        output.seek(0)

    st.success(f"Selesai! Sheet diproses: {len(results)} / {len(xls.sheet_names)}")
    st.subheader("Report per sheet")
    st.dataframe(rep_df, use_container_width=True)

    st.download_button(
        "Download Output (Excel)",
        data=output,
        file_name="Fulfilment_Processed_AH_OP_Release_Enriched.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
