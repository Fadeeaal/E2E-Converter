import io
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="FG Master Data", layout="wide")
st.title("Finish Goods Master Data")

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

# =========================
# Helper Functions
# =========================
def _norm_str(x):
    if x is None: return None
    s = str(x).strip()
    return None if s.lower() in ["nan", "none", "nat", ""] else s

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

def fetch_existing_map(keys: list) -> dict:
    existing_map = {}
    if not keys: return existing_map
    keys = list(set(keys))
    chunk_size = 800
    with engine.connect() as conn:
        for i in range(0, len(keys), chunk_size):
            chunk = keys[i:i + chunk_size]
            values_sql = ", ".join([f"(:s{j}, :r{j})" for j in range(len(chunk))])
            params = {}
            for j, (sku, reg) in enumerate(chunk):
                params[f"s{j}"], params[f"r{j}"] = sku, reg
            sql = text(f"SELECT * FROM fg_master_data WHERE (sku_code, region) IN ({values_sql})")
            rows = conn.execute(sql, params).mappings().all()
            for r in rows:
                existing_map[(str(r["sku_code"]).strip(), str(r["region"]).strip())] = dict(r)
    return existing_map

# =========================
# UI SECTION: MODE SELECTOR
# =========================
tabs = st.tabs(["✏️ Search & Edit", "📤 Bulk Upload"]) 
tab_edit, tab_bulk = tabs

with tab_edit:
    st.subheader("Manual Data Update")
    search_sku = st.text_input("Search SKU Code", placeholder="Enter SKU...").strip()
    
    if search_sku:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT region, line FROM fg_master_data WHERE sku_code = :s"), {"s": search_sku}).mappings().all()
        
        if not rows:
            st.warning("SKU not found.")
        else:
            options = [f"Region: {r['region']} | Line: {r['line']}" for r in rows]
            selected_opt = st.selectbox("Select Line/Region to edit:", options) if len(options) > 1 else options[0]
            
            idx = options.index(selected_opt)
            t_reg, t_line = rows[idx]['region'], rows[idx]['line']
            
            with engine.connect() as conn:
                curr = conn.execute(text("SELECT * FROM fg_master_data WHERE sku_code=:s AND region=:r AND line=:l"), 
                                    {"s": search_sku, "r": t_reg, "l": t_line}).mappings().first()
            
            if curr:
                with st.form("edit_form"):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.text_input("SKU Code", value=curr['sku_code'], disabled=True)
                        desc = st.text_input("Description", value=curr['description'] or "")
                        brand = st.text_input("Brand", value=curr['brand'] or "")
                        sub_brand = st.text_input("Sub Brand", value=curr['sub_brand'] or "")
                        category = st.text_input("Category", value=curr['category'] or "")
                    with c2:
                        size = st.text_input("Size", value=curr['size'] or "")
                        pcs_cb = st.number_input("Pcs/CB", value=float(curr['pcs_cb'] or 0))
                        kg_cb = st.number_input("KG/CB", value=float(curr['kg_cb'] or 0))
                        speed = st.number_input("Speed", value=float(curr['speed'] or 0))
                        output_val = st.text_input("Output", value=curr['output'] or "")

                    if st.form_submit_button("Update Data"):
                        upd_sql = text("""UPDATE fg_master_data SET description=:d, brand=:b, sub_brand=:sb, category=:c, 
                                          size=:s, pcs_cb=:p, kg_cb=:k, speed=:sp, output=:o
                                          WHERE sku_code=:sku AND region=:reg AND line=:line""")
                        with engine.begin() as conn:
                            conn.execute(upd_sql, {"d":desc,"b":brand,"sb":sub_brand,"c":category,"s":size,"p":pcs_cb,"k":kg_cb,"sp":speed,"o":output_val,"sku":search_sku,"reg":t_reg,"line":t_line})
                        st.success("Update Successful!")
                        st.rerun()

    st.markdown("---")
    
    df_all = load_db()
    c1, _, c3 = st.columns([1, 8, 1])
    with c1:
        st.metric("Total Data", len(df_all))
    with c3:
        st.download_button("📥 Download", data=convert_df_to_excel(df_all), file_name="FG_Master_Data_Full.xlsx")

    st.dataframe(df_all, use_container_width=True)

with tab_bulk:
    st.subheader("Bulk Sync via Excel")
    uploaded = st.file_uploader("Upload Excel (Sheet: 'Database FG')", type=["xlsx"])
    if uploaded:
        df_up = pd.read_excel(uploaded, sheet_name="Database FG")
        df_up = df_up.rename(columns=EXCEL_ALIASES).rename(columns=EXCEL_MAPPING)
        st.dataframe(df_up.head(10))
        
        if st.button("Sync to Database"):
            with st.spinner("Analyzing..."):
                df_up['sku_code'] = df_up['sku_code'].astype(str).str.strip()
                df_up['region'] = df_up['region'].astype(str).str.strip().str.upper()
                existing_map = fetch_existing_map(list(zip(df_up['sku_code'], df_up['region'])))
                
                to_ins, to_upd = [], []
                for _, row in df_up.iterrows():
                    key = (row['sku_code'], row['region'])
                    d = row.to_dict()
                    if key not in existing_map: to_ins.append(d)
                    else: to_upd.append(d)
                
                with engine.begin() as conn:
                    if to_ins:
                        conn.execute(text("""INSERT INTO fg_master_data (sku_code, description, region, line, brand, sub_brand, category, size, pcs_cb, kg_cb, speed, output) 
                                             VALUES (:sku_code, :description, :region, :line, :brand, :sub_brand, :category, :size, :pcs_cb, :kg_cb, :speed, :output)"""), to_ins)
                    if to_upd:
                        conn.execute(text("""UPDATE fg_master_data SET description=:description, line=:line, brand=:brand, sub_brand=:sub_brand, category=:category, 
                                             size=:size, pcs_cb=:pcs_cb, kg_cb=:kg_cb, speed=:speed, output=:output
                                             WHERE sku_code=:sku_code AND region=:region"""), to_upd)
                st.success(f"Sync Done: {len(to_ins)} Inserted, {len(to_upd)} Updated.")
                st.rerun()

st.sidebar.subheader("⚠️ DELETE ALL DATA")
confirm = st.sidebar.checkbox("This will permanently delete all FG master data.")

if confirm:
    if st.sidebar.button("DELETE"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE fg_master_data"))
        st.rerun()