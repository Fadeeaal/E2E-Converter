import io
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="FG Master Data", layout="wide")
st.title("FG Master Data — fg_master_data (All Regions)")

# =========================
# DB Engine
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

# =========================
# Column schema (fg_master_data)
# =========================
DB_COLS = [
    "sku_code",
    "description",
    "country",
    "brand",
    "sub_brand",
    "category",
    "big_category",
    "house",
    "region",
    "line",
    "size",
    "pcs_cb",
    "kg_cb",
    "speed",
    "pack_format",
    "output",
]

# Excel headers -> DB columns
# (Silakan sesuaikan header excel kamu di sini)
EXCEL_MAPPING = {
    "SKU Code": "sku_code",
    "Description": "description",
    "Country": "country",
    "Brand": "brand",
    "Sub Brand": "sub_brand",
    "Category": "category",
    "Big Category": "big_category",
    "House": "house",
    "Region": "region",
    "Line": "line",
    "Size": "size",
    "Pcs/CB": "pcs_cb",
    "KG/CB": "kg_cb",
    "Speed": "speed",
    "Pack Format": "pack_format",
    "Output": "output",
}

# (opsional) alias header lama supaya file excel lama tetap kebaca
EXCEL_ALIASES = {
    "Material": "SKU Code",
    "Material Description": "Description",
    "Subbrand": "Sub Brand",
    "Pcs/cb": "Pcs/CB",
    "Pack format": "Pack Format",
}

def _norm_str(x):
    if x is None:
        return None
    s = str(x).strip()
    if s.lower() in ["nan", "none", "nat", ""]:
        return None
    return s

def _coerce_number(x):
    if x is None:
        return None
    if isinstance(x, (int, float)) and pd.notna(x):
        return float(x)
    s = str(x).strip()
    if s.lower() in ["nan", "none", "nat", ""]:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

# =========================
# DB helpers
# =========================
def load_db(limit: int = 20000) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(
            text("""
                SELECT *
                FROM fg_master_data
                ORDER BY region, line, sku_code
                LIMIT :lim
            """),
            conn,
            params={"lim": limit},
        )

def convert_df_to_excel(df: pd.DataFrame) -> bytes:
    df_clean = df.copy()

    # drop typical audit cols if exist
    cols_to_remove = ["id", "created_at", "updated_at"]
    df_clean = df_clean.drop(columns=[c for c in cols_to_remove if c in df_clean.columns])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_clean.to_excel(writer, index=False, sheet_name="Database FG")
    return output.getvalue()

def excel_to_db_df(uploaded) -> pd.DataFrame:
    df = pd.read_excel(uploaded, sheet_name="Database FG", engine="openpyxl")

    # normalize legacy headers (optional)
    rename_alias = {}
    for c in df.columns:
        if c in EXCEL_ALIASES:
            rename_alias[c] = EXCEL_ALIASES[c]
    if rename_alias:
        df = df.rename(columns=rename_alias)

    missing = [c for c in EXCEL_MAPPING.keys() if c not in df.columns]
    if missing:
        raise ValueError(f"Kolom Excel ini tidak ditemukan: {missing}")

    df = df.rename(columns=EXCEL_MAPPING)
    df = df[list(EXCEL_MAPPING.values())].copy()

    # normalize strings
    for c in df.columns:
        df[c] = df[c].apply(_norm_str)

    # sku_code wajib
    df = df[df["sku_code"].notna()].copy()
    df["sku_code"] = df["sku_code"].astype(str).str.strip()

    # region wajib (karena table fg_master_data punya region)
    if "region" not in df.columns or df["region"].isna().any():
        raise ValueError("Kolom 'Region' wajib diisi untuk semua baris (WEST/EAST/dll).")

    # coerce numeric fields
    for nc in ["pcs_cb", "kg_cb", "speed"]:
        if nc in df.columns:
            df[nc] = df[nc].apply(_coerce_number)

    return df

def fetch_existing_map(keys: list[tuple[str, str | None]]) -> dict:
    """
    keys: list of (sku_code, region)
    Return: {(sku_code, region): row_dict}
    """
    existing_map = {}
    if not keys:
        return existing_map

    # de-dup
    keys = list({(str(s).strip(), None if r is None else str(r).strip()) for s, r in keys})

    # Query in chunks to avoid huge IN
    chunk_size = 800
    with engine.connect() as conn:
        for i in range(0, len(keys), chunk_size):
            chunk = keys[i:i + chunk_size]

            # Build a VALUES table for composite key join (safer than IN tuples in some DBs)
            # Example: VALUES ('A','WEST'), ('B','EAST')
            values_sql = ", ".join(
                [f"(:s{i+j}, :r{i+j})" for j in range(len(chunk))]
            )
            params = {}
            for j, (sku, reg) in enumerate(chunk):
                params[f"s{i+j}"] = sku
                params[f"r{i+j}"] = reg

            sql_fetch = text(f"""
                SELECT m.*
                FROM fg_master_data m
                JOIN (VALUES {values_sql}) AS v(sku_code, region)
                  ON m.sku_code = v.sku_code AND m.region = v.region
            """)
            rows = conn.execute(sql_fetch, params).mappings().all()
            for r in rows:
                existing_map[(str(r["sku_code"]).strip(), str(r["region"]).strip())] = dict(r)

    return existing_map

# =========================
# UI — Bulk Upload
# =========================
st.subheader("Bulk Upload Excel (All Regions)")

uploaded = st.file_uploader("Upload Excel Master Data (.xlsx) — sheet name: 'Database FG'", type=["xlsx"])

if uploaded:
    try:
        df_up = excel_to_db_df(uploaded)

        st.write("Preview (top 30):")
        st.dataframe(df_up.head(30), use_container_width=True)

        total_rows = len(df_up)
        st.caption(f"Total rows in file: {total_rows:,}")

        if st.button("Upload to DB (Insert New & Update Existing)"):
            with st.spinner("Analyzing data differences..."):
                # key = (sku_code, region)
                df_up["region"] = df_up["region"].astype(str).str.strip().str.upper()
                df_up["sku_code"] = df_up["sku_code"].astype(str).str.strip()

                keys = list(zip(df_up["sku_code"], df_up["region"]))
                existing_map = fetch_existing_map(keys)

                to_insert = []
                to_update = []
                skipped_count = 0

                check_cols = [c for c in DB_COLS if c != "sku_code"]  # region is included here (but key is sku+region)

                for _, row in df_up.iterrows():
                    sku = str(row["sku_code"]).strip()
                    reg = str(row["region"]).strip().upper()
                    row_dict = row.to_dict()

                    key = (sku, reg)
                    if key not in existing_map:
                        to_insert.append(row_dict)
                        continue

                    db_row = existing_map[key]

                    # compare all non-key cols (exclude sku_code; region part of key so also exclude)
                    is_different = False
                    for col in [c for c in DB_COLS if c not in ["sku_code", "region"]]:
                        val_excel = row_dict.get(col)
                        val_db = db_row.get(col)

                        if col in ["pcs_cb", "kg_cb", "speed"]:
                            ve = _coerce_number(val_excel)
                            vd = _coerce_number(val_db)
                            if ve != vd:
                                is_different = True
                                break
                        else:
                            se = "" if val_excel is None else str(val_excel).strip()
                            sd = "" if val_db is None else str(val_db).strip()
                            if se != sd:
                                is_different = True
                                break

                    if is_different:
                        to_update.append(row_dict)
                    else:
                        skipped_count += 1

            # INSERT
            msg_insert = ""
            msg_update = ""

            if to_insert:
                insert_sql = text("""
                    INSERT INTO fg_master_data
                    (sku_code, description, pcs_cb, kg_cb, size, country, brand, sub_brand,
                     category, big_category, house, region, speed, pack_format, line, output)
                    VALUES
                    (:sku_code, :description, :pcs_cb, :kg_cb, :size, :country, :brand, :sub_brand,
                     :category, :big_category, :house, :region, :speed, :pack_format, :line, :output)
                """)
                try:
                    with engine.begin() as conn:
                        conn.execute(insert_sql, to_insert)
                    msg_insert = f"✅ Insert: {len(to_insert)} row baru."
                except Exception as e:
                    st.error(f"Error Insert: {e}")

            # UPDATE (key = sku_code + region)
            if to_update:
                update_sql = text("""
                    UPDATE fg_master_data
                    SET
                        description = :description,
                        pcs_cb = :pcs_cb,
                        kg_cb = :kg_cb,
                        size = :size,
                        country = :country,
                        brand = :brand,
                        sub_brand = :sub_brand,
                        category = :category,
                        big_category = :big_category,
                        house = :house,
                        speed = :speed,
                        pack_format = :pack_format,
                        line = :line,
                        output = :output
                    WHERE sku_code = :sku_code
                      AND region = :region
                """)
                try:
                    with engine.begin() as conn:
                        conn.execute(update_sql, to_update)
                    msg_update = f"✏️ Update: {len(to_update)} row berubah."
                except Exception as e:
                    st.error(f"Error Update: {e}")

            st.success("Upload selesai.")
            if msg_insert:
                st.write(msg_insert)
            if msg_update:
                st.write(msg_update)
            if skipped_count > 0:
                st.info(f"Skipped: {skipped_count} row (tidak ada perubahan).")

            if to_insert or to_update:
                st.rerun()

    except Exception as e:
        st.error(f"Gagal memproses file: {e}")
else:
    st.caption("Upload Excel untuk bulk upload (Insert & Update) untuk seluruh region.")

# =========================
# Preview + Download
# =========================
st.markdown("---")
st.subheader("Database Preview (All Regions)")

limit = st.number_input("Preview limit", min_value=100, max_value=200000, value=20000, step=1000)
df_for_download = load_db(limit=int(limit))

c1, c2 = st.columns([1, 2])
with c1:
    st.metric("Rows (Preview)", len(df_for_download))

with c2:
    if not df_for_download.empty:
        excel_data = convert_df_to_excel(df_for_download)
        st.download_button(
            label="📥 Download Preview as Excel",
            data=excel_data,
            file_name="FG_Master_Data_Export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.warning("Database kosong, tidak ada data untuk diunduh.")

st.dataframe(df_for_download, use_container_width=True, height=520)

# =========================
# Danger Zone (ALL DATA)
# =========================
st.sidebar.subheader("⚠️ Danger Zone (ALL DATA)")
st.sidebar.caption("Ini akan menghapus SEMUA baris di fg_master_data.")
confirm = st.sidebar.checkbox("Yes, I want to delete ALL data in fg_master_data.")
if st.sidebar.button("DELETE ALL (truncate fg_master_data)") and confirm:
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE fg_master_data"))
        st.sidebar.success("All data cleared (TRUNCATE).")
        st.rerun()
    except Exception as e:
        st.sidebar.error(f"Failed to truncate: {e}")
