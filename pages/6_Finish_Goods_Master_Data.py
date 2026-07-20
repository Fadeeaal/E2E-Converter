import io
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="FG Master Data", layout="wide")
st.title("Finish Goods Master Data")

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

DB_COLS = [
    "sku_code", "description", "country", "brand", "sub_brand",
    "category", "big_category", "house", "region", "line",
    "size", "pcs_cb", "kg_cb", "speed", "pack_format", "output"
]

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

EXCEL_ALIASES = {
    "Material": "SKU Code",
    "Material Description": "Description",
    "Subbrand": "Sub Brand",
    "Pcs/cb": "Pcs/CB",
    "Pack format": "Pack Format",
}

# ---------------------------------------------------------------------------
# Normalisasi & helper
# ---------------------------------------------------------------------------
# PRIMARY KEY ASLI tabel fg_master_data (dikonfirmasi dari constraint
# "fg_master_data_pkey" lewat error UniqueViolation di production):
#   (sku_code, line, pcs_cb, kg_cb)
# CATATAN: region TIDAK termasuk primary key. Artinya kombinasi
# sku_code+line+pcs_cb+kg_cb yang sama akan dianggap SATU baris,
# walau datanya datang dari region berbeda. Kalau ini bukan yang
# diinginkan secara bisnis, primary key di level database perlu
# diubah supaya ikut menyertakan region.
PK_COLS = ["sku_code", "line", "pcs_cb", "kg_cb"]

def _norm_str(x):
    if x is None: return None
    s = str(x).strip()
    return None if s.lower() in ["nan", "none", "nat", ""] else s

def _norm_key_part(x):
    """Normalisasi satu bagian key teks (sku_code / line) supaya konsisten
    antara data Excel dan data yang diambil dari DB."""
    return _norm_str(x)

def _norm_num(x):
    """Normalisasi satu bagian key numerik (pcs_cb / kg_cb). Dibulatkan
    supaya nilai float dari Excel dan dari Postgres tidak dianggap
    berbeda karena selisih presisi floating point."""
    if x is None:
        return None
    try:
        v = float(x)
        if pd.isna(v):
            return None
        return round(v, 4)
    except (TypeError, ValueError):
        return None

def _coerce_number(x):
    if x is None: return None
    if isinstance(x, (int, float)) and pd.notna(x): return float(x)
    s = str(x).strip().replace(",", ".")
    try: return float(s)
    except: return None

def load_db() -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text("SELECT * FROM fg_master_data ORDER BY region, line, sku_code"), conn)

def convert_df_to_excel(df: pd.DataFrame) -> bytes:
    df_clean = df.copy()
    cols_to_remove = ["id", "created_at", "updated_at"]
    df_clean = df_clean.drop(columns=[c for c in cols_to_remove if c in df_clean.columns])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_clean.to_excel(writer, index=False, sheet_name="Database FG")
    return output.getvalue()

def fetch_existing_map() -> dict:
    """
    Ambil SELURUH tabel (bukan query per-key dengan ratusan bind-parameter)
    dan key-kan dengan primary key ASLI tabel: (sku_code, line, pcs_cb, kg_cb).

    Kenapa full-table fetch, bukan query dinamis WHERE ... IN (...):
    - Untuk ~700-1000 baris data, ini jauh lebih murah & 100% robust.
    - Menghindari semua masalah type-inference Postgres (ProgrammingError),
      row-constructor IN yang rewel dengan NULL (UndefinedFunction), dan
      parsing bind-parameter raksasa yang gagal (SyntaxError) -- semuanya
      pernah muncul saat mencoba pendekatan query dinamis.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM fg_master_data")).mappings().all()
    existing_map = {}
    for r in rows:
        key = (
            _norm_key_part(r.get("sku_code")),
            _norm_key_part(r.get("line")),
            _norm_num(r.get("pcs_cb")),
            _norm_num(r.get("kg_cb")),
        )
        existing_map[key] = dict(r)
    return existing_map

tabs = st.tabs(["Search & Edit Data", "Add Material Data"])
tab_edit, tab_bulk = tabs

with tab_edit:
    st.subheader("Manual Data Update")
    search_sku = st.text_input("Search SKU Code", placeholder="Enter SKU...").strip()

    if search_sku:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT region, line, pcs_cb, kg_cb FROM fg_master_data WHERE sku_code = :s"),
                {"s": search_sku}
            ).mappings().all()

        if not rows:
            st.warning("SKU not found.")
        else:
            options = [
                f"Region: {r['region']} | Line: {r['line']} | Pcs/CB: {r['pcs_cb']} | KG/CB: {r['kg_cb']}"
                for r in rows
            ]
            selected_opt = st.selectbox("Select Row to edit:", options) if len(options) > 1 else options[0]

            idx = options.index(selected_opt)
            t_line = rows[idx]['line']
            t_pcs_cb = rows[idx]['pcs_cb']
            t_kg_cb = rows[idx]['kg_cb']

            with engine.connect() as conn:
                curr = conn.execute(
                    text("""SELECT * FROM fg_master_data
                            WHERE sku_code=:s AND line=:l AND pcs_cb=:p AND kg_cb=:k"""),
                    {"s": search_sku, "l": t_line, "p": t_pcs_cb, "k": t_kg_cb}
                ).mappings().first()

            if curr:
                with st.form("edit_form"):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.text_input("SKU Code", value=curr['sku_code'], disabled=True)
                        st.text_input("Line", value=str(curr['line'] or ""), disabled=True)
                        desc = st.text_input("Description", value=curr['description'] or "")
                        region = st.text_input("Region", value=curr['region'] or "")
                        brand = st.text_input("Brand", value=curr['brand'] or "")
                        sub_brand = st.text_input("Sub Brand", value=curr['sub_brand'] or "")
                        category = st.text_input("Category", value=curr['category'] or "")
                    with c2:
                        size = st.text_input("Size", value=curr['size'] or "")
                        st.number_input("Pcs/CB (bagian dari primary key, tidak bisa diedit)",
                                         value=float(curr['pcs_cb'] or 0), disabled=True)
                        st.number_input("KG/CB (bagian dari primary key, tidak bisa diedit)",
                                         value=float(curr['kg_cb'] or 0), disabled=True)
                        speed = st.number_input("Speed", value=float(curr['speed'] or 0))
                        output_val = st.text_input("Output", value=curr['output'] or "")

                    st.caption("Pcs/CB dan KG/CB tidak bisa diedit di sini karena keduanya "
                               "bagian dari primary key tabel. Gunakan fitur Bulk Sync kalau "
                               "nilainya memang perlu diubah.")

                    if st.form_submit_button("Update Data"):
                        upd_sql = text("""UPDATE fg_master_data SET description=:d, region=:reg, brand=:b, sub_brand=:sb, category=:c,
                                          size=:s, speed=:sp, output=:o
                                          WHERE sku_code=:sku AND line=:line AND pcs_cb=:p AND kg_cb=:k""")
                        with engine.begin() as conn:
                            conn.execute(upd_sql, {
                                "d": desc, "reg": region, "b": brand, "sb": sub_brand, "c": category,
                                "s": size, "sp": speed, "o": output_val,
                                "sku": search_sku, "line": t_line, "p": t_pcs_cb, "k": t_kg_cb
                            })
                        st.success("Update Successful!")
                        st.rerun()

    st.markdown("---")

    df_all = load_db()
    c1, _, c3 = st.columns([1, 8, 1])
    with c1:
        st.metric("Total Data", len(df_all))
    with c3:
        st.download_button("Download", data=convert_df_to_excel(df_all), file_name="FG_Master_Data_Full.xlsx")

    # Fix tampilan: kolom 'line' kadang berisi campuran string & angka (mis. 0, 'CAN'),
    # yang bikin Streamlit gagal serialize ke Arrow. Paksa semua jadi string untuk tampilan.
    df_display = df_all.copy()
    if "line" in df_display.columns:
        df_display["line"] = df_display["line"].apply(lambda x: "" if pd.isna(x) else str(x))
    st.dataframe(df_display, width="stretch")

with tab_bulk:
    st.subheader("Single Add Material Data")
    with st.form("single_add_form"):
        c1, c2 = st.columns(2)
        with c1:
            sku_code = st.text_input("SKU Code").strip()
            description = st.text_input("Description")
            region = st.text_input("Region").strip().upper()
            line = st.text_input("Line")
            brand = st.text_input("Brand")
            sub_brand = st.text_input("Sub Brand")
        with c2:
            category = st.text_input("Category")
            size = st.text_input("Size")
            pcs_cb = st.number_input("Pcs/CB", value=0.0)
            kg_cb = st.number_input("KG/CB", value=0.0)
            speed = st.number_input("Speed", value=0.0)
            output = st.text_input("Output")

        submitted = st.form_submit_button("Save Single Material")

    if submitted:
        if not sku_code or not region:
            st.error("SKU Code dan Region wajib diisi.")
        else:
            single = {
                "sku_code": sku_code,
                "description": description,
                "region": region,
                "line": line,
                "brand": brand,
                "sub_brand": sub_brand,
                "category": category,
                "size": size,
                "pcs_cb": pcs_cb,
                "kg_cb": kg_cb,
                "speed": speed,
                "output": output,
            }
            key = (_norm_key_part(sku_code), _norm_key_part(line), _norm_num(pcs_cb), _norm_num(kg_cb))
            existing_map = fetch_existing_map()
            with engine.begin() as conn:
                if key not in existing_map:
                    conn.execute(text("""INSERT INTO fg_master_data
                        (sku_code, description, region, line, brand, sub_brand, category, size, pcs_cb, kg_cb, speed, output)
                        VALUES (:sku_code, :description, :region, :line, :brand, :sub_brand, :category, :size, :pcs_cb, :kg_cb, :speed, :output)
                    """), single)
                    st.success("Material berhasil ditambahkan.")
                else:
                    conn.execute(text("""UPDATE fg_master_data SET
                        description=:description, region=:region, brand=:brand, sub_brand=:sub_brand, category=:category,
                        size=:size, speed=:speed, output=:output
                        WHERE sku_code=:sku_code AND line=:line AND pcs_cb=:pcs_cb AND kg_cb=:kg_cb
                    """), single)
                    st.success("Material berhasil di-update.")
            st.rerun()

    st.markdown("---")
    st.subheader("Bulk Add Material Data via Excel")
    uploaded = st.file_uploader("Upload Excel (Sheet: 'Database FG')", type=["xlsx"])
    if uploaded:
        df_up = pd.read_excel(uploaded, sheet_name="Database FG")
        df_up = df_up.rename(columns=EXCEL_ALIASES).rename(columns=EXCEL_MAPPING)
        st.dataframe(df_up.head(10))

        if st.button("Sync to Database"):
            with st.spinner("Analyzing..."):
                # Normalisasi kolom-kolom yang jadi bagian primary key
                df_up['sku_code'] = df_up['sku_code'].apply(_norm_key_part)
                df_up['region'] = df_up['region'].astype(str).str.strip().str.upper()
                df_up['line'] = df_up['line'].apply(_norm_key_part)
                df_up['pcs_cb'] = df_up['pcs_cb'].apply(_norm_num)
                df_up['kg_cb'] = df_up['kg_cb'].apply(_norm_num)

                # Buang duplikat SEJATI pada primary key asli tabel
                dup_mask = df_up.duplicated(subset=['sku_code', 'line', 'pcs_cb', 'kg_cb'], keep=False)
                n_dupes = dup_mask.sum()
                if n_dupes > 0:
                    st.warning(f"⚠️ {n_dupes} baris punya kombinasi SKU+Line+Pcs/CB+KG/CB identik "
                               f"(primary key tabel). Baris terakhir yang dipakai.")
                    st.dataframe(df_up[dup_mask].sort_values(['sku_code', 'line']))
                df_up = df_up.drop_duplicates(subset=['sku_code', 'line', 'pcs_cb', 'kg_cb'], keep='last')

                # Fetch seluruh tabel sekali saja -- tanpa bind-parameter dinamis
                existing_map = fetch_existing_map()

                to_ins, to_upd = [], []
                for _, row in df_up.iterrows():
                    key = (row['sku_code'], row['line'], row['pcs_cb'], row['kg_cb'])
                    d = row.to_dict()
                    if key not in existing_map:
                        to_ins.append(d)
                    else:
                        to_upd.append(d)

                with engine.begin() as conn:
                    if to_ins:
                        conn.execute(text("""INSERT INTO fg_master_data (sku_code, description, region, line, brand, sub_brand, category, size, pcs_cb, kg_cb, speed, output)
                                            VALUES (:sku_code, :description, :region, :line, :brand, :sub_brand, :category, :size, :pcs_cb, :kg_cb, :speed, :output)"""), to_ins)
                    if to_upd:
                        # sku_code, line, pcs_cb, kg_cb TIDAK di-SET karena itu primary key -> hanya di WHERE
                        conn.execute(text("""UPDATE fg_master_data SET description=:description, region=:region, brand=:brand, sub_brand=:sub_brand, category=:category,
                                            size=:size, speed=:speed, output=:output
                                            WHERE sku_code=:sku_code AND line=:line AND pcs_cb=:pcs_cb AND kg_cb=:kg_cb"""), to_upd)
                st.success(f"Sync Done: {len(to_ins)} Inserted, {len(to_upd)} Updated.")
                st.rerun()

st.sidebar.subheader("⚠️ DELETE ALL DATA")
confirm = st.sidebar.checkbox("This will permanently delete all FG master data.")

if confirm and st.sidebar.button("DELETE"):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fg_master_data"))
    st.rerun()