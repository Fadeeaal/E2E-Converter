import pandas as pd
import streamlit as st
st.set_page_config(page_title="Master Data (West)", layout="wide")
st.title("Master Data (West)")

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

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
    "material",
    "material_description",
    "country",
    "brand",
    "sub_brand",
    "category",
    "big_category",
    "house",
    "size",
    "pcs_cb",
    "kg_cb",
    "pack_format",
    "size_format",
    "insource_or_outsource",
    "machine_1",
]

EXCEL_MAPPING = {
    "Material": "material",
    "Material Description": "material_description",
    "Country": "country",
    "Brand": "brand",
    "Subbrand": "sub_brand",
    "Category": "category",
    "Big Category": "big_category",
    "House": "house",
    "Size": "size",
    "Pcs/cb": "pcs_cb",
    "KG/CB": "kg_cb",
    "Pack format": "pack_format",
    "Size format": "size_format",
    "Insource / Outsource": "insource_or_outsource",
    "Machine 1": "machine_1",
}

def load_db(limit: int = 5000) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(
            text("SELECT * FROM zcorin_converter ORDER BY id DESC LIMIT :lim"),
            conn,
            params={"lim": limit},
        )

def get_row_by_material(material: str):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM zcorin_converter WHERE material = :m LIMIT 1"),
            {"m": material},
        ).mappings().first()
    return dict(row) if row else None

def insert_row(payload: dict):
    sql = text("""
    INSERT INTO zcorin_converter
    (material, material_description, country, brand, sub_brand, category, big_category, house, size,
     pcs_cb, kg_cb, pack_format, size_format, insource_or_outsource, machine_1, updated_at)
    VALUES
    (:material, :material_description, :country, :brand, :sub_brand, :category, :big_category, :house, :size,
     :pcs_cb, :kg_cb, :pack_format, :size_format, :insource_or_outsource, :machine_1, NOW())
    """)
    with engine.begin() as conn:
        conn.execute(sql, payload)

def update_only_changed(material: str, old_row: dict, new_values: dict):
    changed = {}
    for k in DB_COLS:
        if k == "material":
            continue
        old_v = old_row.get(k)
        new_v = new_values.get(k)
        if isinstance(old_v, str):
            old_v = old_v.strip()
        if isinstance(new_v, str):
            new_v = new_v.strip()
        if old_v != new_v:
            changed[k] = new_v
    if not changed:
        return 0, []
    set_parts = [f"{col} = :{col}" for col in changed.keys()]
    sql = text(f"""
        UPDATE zcorin_converter
        SET {", ".join(set_parts)}, updated_at = NOW()
        WHERE material = :material
    """)
    params = {"material": material, **changed}
    with engine.begin() as conn:
        res = conn.execute(sql, params)
    return res.rowcount, list(changed.keys())

def excel_to_db_df(uploaded) -> pd.DataFrame:
    df = pd.read_excel(uploaded, sheet_name="Database FG", engine="openpyxl")
    missing = [c for c in EXCEL_MAPPING.keys() if c not in df.columns]
    if missing:
        raise ValueError(f"Kolom Excel ini tidak ditemukan: {missing}")
    df = df.rename(columns=EXCEL_MAPPING)
    df = df[list(EXCEL_MAPPING.values())].copy()
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
        df.loc[df[c].isin(["nan", "None", "NaT", ""]), c] = None
    df = df[df["material"].notna()].copy()
    df["material"] = df["material"].astype(str).str.strip()
    return df

def get_existing_materials_set() -> set:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT material FROM zcorin_converter")).fetchall()
    return set(r[0] for r in rows if r and r[0] is not None)

# =========================
# UI SECTION
# =========================

# Mode selector (Edit/Add)
mode = st.radio(
    "Select Mode",
    options=["✏️ Edit Existing", "➕ Add New"],
    horizontal=True,
    help="Switch between editing existing records or adding new ones"
)

st.markdown("---")

# tampilkan flash message jika ada (setelah update)
if st.session_state.get("update_flash"):
    st.success(st.session_state.pop("update_flash"))

# =========================
# EDIT MODE
# =========================
if mode == "✏️ Edit Existing":
    st.markdown("### Search & Edit")
    # Get all materials for dropdown
    with st.spinner("Loading materials..."):
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT DISTINCT material FROM zcorin_converter WHERE material IS NOT NULL ORDER BY material")).fetchall()
        all_materials = [r[0] for r in rows if r and r[0] is not None]

    if all_materials:
        search_material = st.selectbox(
            "Select Material",
            options=["-- Select --"] + all_materials,
            index=0,
            help="Select a material to edit"
        )
        if search_material == "-- Select --":
            search_material = None
    else:
        st.warning("No materials in database yet.")
        search_material = None

    if search_material:
        row = get_row_by_material(search_material)
        if row:
            st.success(f"Found Material **{search_material}**")
            with st.form("edit_existing"):
                c1, c2 = st.columns(2)
                with c1:
                    st.text_input("Material *", value=row.get("material"), disabled=True)
                    material_description = st.text_input("Material Description", value=row.get("material_description") or "")
                    country = st.text_input("Country", value=row.get("country") or "")
                    brand = st.text_input("Brand", value=row.get("brand") or "")
                    sub_brand = st.text_input("Subbrand", value=row.get("sub_brand") or "")
                    category = st.text_input("Category", value=row.get("category") or "")
                    big_category = st.text_input("Big Category", value=row.get("big_category") or "")
                with c2:
                    house = st.text_input("House", value=row.get("house") or "")
                    size = st.text_input("Size", value=row.get("size") or "")
                    pcs_cb = st.text_input("Pcs/cb", value=row.get("pcs_cb") or "")
                    kg_cb = st.text_input("KG/CB", value=row.get("kg_cb") or "")
                    pack_format = st.text_input("Pack format", value=row.get("pack_format") or "")
                    size_format = st.text_input("Size format", value=row.get("size_format") or "")
                    insource_or_outsource = st.text_input("Insource / Outsource", value=row.get("insource_or_outsource") or "")
                    machine_1 = st.text_input("Machine 1", value=row.get("machine_1") or "")

                submitted = st.form_submit_button("Update Material")
                if submitted:
                    new_values = {
                        "material": row["material"],
                        "material_description": material_description.strip() or None,
                        "country": country.strip() or None,
                        "brand": brand.strip() or None,
                        "sub_brand": sub_brand.strip() or None,
                        "category": category.strip() or None,
                        "big_category": big_category.strip() or None,
                        "house": house.strip() or None,
                        "size": size.strip() or None,
                        "pcs_cb": pcs_cb.strip() or None,
                        "kg_cb": kg_cb.strip() or None,
                        "pack_format": pack_format.strip() or None,
                        "size_format": size_format.strip() or None,
                        "insource_or_outsource": insource_or_outsource.strip() or None,
                        "machine_1": machine_1.strip() or None,
                    }
                    try:
                        count, changed_cols = update_only_changed(row["material"], row, new_values)
                        if count == 0:
                            st.info("No changes detected (no update executed).")
                        else:
                            changed_items = []
                            for col in changed_cols:
                                old_v = row.get(col)
                                new_v = new_values.get(col)
                                changed_items.append(f"{col}: '{old_v}' -> '{new_v}'")
                            st.session_state["update_flash"] = (
                                f"Material {row['material']} updated successfully! Changes: "
                                + "; ".join(changed_items)
                            )
                            st.rerun()
                    except Exception as e:
                        st.error(f"Gagal update: {e}")
        else:
            st.warning("Material not found in database.")

# =========================
# ADD MODE
# =========================
elif mode == "➕ Add New":
    st.markdown("### ➕ Add New Material")
    with st.form("add_new_form"):
        c1, c2 = st.columns(2)
        with c1:
            new_material = st.text_input("Material *", placeholder="Contoh: 1234567").strip()
            material_description = st.text_input("Material Description")
            country = st.text_input("Country")
            brand = st.text_input("Brand")
            sub_brand = st.text_input("Subbrand")
            category = st.text_input("Category")
            big_category = st.text_input("Big Category")
        with c2:
            house = st.text_input("House")
            size = st.text_input("Size")
            pcs_cb = st.text_input("Pcs/cb")
            kg_cb = st.text_input("KG/CB")
            pack_format = st.text_input("Pack format")
            size_format = st.text_input("Size format")
            insource_or_outsource = st.text_input("Insource / Outsource")
            machine_1 = st.text_input("Machine 1")

        submitted = st.form_submit_button("Insert New Material")
        if submitted:
            if not new_material:
                st.warning("Material wajib diisi.")
            else:
                # Cek duplikat
                if get_row_by_material(new_material):
                    st.error(f"Material {new_material} sudah ada di database. Gunakan Edit mode untuk mengubah.")
                else:
                    payload = {
                        "material": new_material,
                        "material_description": material_description.strip() or None,
                        "country": country.strip() or None,
                        "brand": brand.strip() or None,
                        "sub_brand": sub_brand.strip() or None,
                        "category": category.strip() or None,
                        "big_category": big_category.strip() or None,
                        "house": house.strip() or None,
                        "size": size.strip() or None,
                        "pcs_cb": pcs_cb.strip() or None,
                        "kg_cb": kg_cb.strip() or None,
                        "pack_format": pack_format.strip() or None,
                        "size_format": size_format.strip() or None,
                        "insource_or_outsource": insource_or_outsource.strip() or None,
                        "machine_1": machine_1.strip() or None,
                    }
                    try:
                        insert_row(payload)
                        st.success("Insert berhasil.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal insert: {e}")

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
    "material",
    "material_description",
    "country",
    "brand",
    "sub_brand",
    "category",
    "big_category",
    "house",
    "size",
    "pcs_cb",
    "kg_cb",
    "pack_format",
    "size_format",
    "insource_or_outsource",
    "machine_1",
]

EXCEL_MAPPING = {
    "Material": "material",
    "Material Description": "material_description",
    "Country": "country",
    "Brand": "brand",
    "Subbrand": "sub_brand",
    "Category": "category",
    "Big Category": "big_category",
    "House": "house",
    "Size": "size",
    "Pcs/cb": "pcs_cb",
    "KG/CB": "kg_cb",
    "Pack format": "pack_format",
    "Size format": "size_format",
    "Insource / Outsource": "insource_or_outsource",
    "Machine 1": "machine_1",
}

def load_db(limit: int = 5000) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(
            text("SELECT * FROM zcorin_converter ORDER BY id DESC LIMIT :lim"),
            conn,
            params={"lim": limit},
        )

def get_row_by_material(material: str):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM zcorin_converter WHERE material = :m LIMIT 1"),
            {"m": material},
        ).mappings().first()
    return dict(row) if row else None

def insert_row(payload: dict):
    sql = text("""
    INSERT INTO zcorin_converter
    (material, material_description, country, brand, sub_brand, category, big_category, house, size,
     pcs_cb, kg_cb, pack_format, size_format, insource_or_outsource, machine_1, updated_at)
    VALUES
    (:material, :material_description, :country, :brand, :sub_brand, :category, :big_category, :house, :size,
     :pcs_cb, :kg_cb, :pack_format, :size_format, :insource_or_outsource, :machine_1, NOW())
    """)
    with engine.begin() as conn:
        conn.execute(sql, payload)

def update_only_changed(material: str, old_row: dict, new_values: dict):
    """
    Update hanya kolom yang berubah.
    old_row: row existing dari DB
    new_values: dict kolom->nilai baru (kolom DB_COLS)
    """
    changed = {}
    for k in DB_COLS:
        if k == "material":
            continue
        old_v = old_row.get(k)
        new_v = new_values.get(k)

        # normalize whitespace for string compare
        if isinstance(old_v, str):
            old_v = old_v.strip()
        if isinstance(new_v, str):
            new_v = new_v.strip()

        if old_v != new_v:
            changed[k] = new_v

    if not changed:
        return 0, []

    # build dynamic UPDATE query
    set_parts = [f"{col} = :{col}" for col in changed.keys()]
    sql = text(f"""
        UPDATE zcorin_converter
        SET {", ".join(set_parts)}, updated_at = NOW()
        WHERE material = :material
    """)

    params = {"material": material, **changed}

    with engine.begin() as conn:
        res = conn.execute(sql, params)

    return res.rowcount, list(changed.keys())

def excel_to_db_df(uploaded) -> pd.DataFrame:
    df = pd.read_excel(uploaded, sheet_name="Database FG", engine="openpyxl")
    missing = [c for c in EXCEL_MAPPING.keys() if c not in df.columns]
    if missing:
        raise ValueError(f"Kolom Excel ini tidak ditemukan: {missing}")

    df = df.rename(columns=EXCEL_MAPPING)
    df = df[list(EXCEL_MAPPING.values())].copy()

    # Normalize: strip + convert NaN -> None
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
        df.loc[df[c].isin(["nan", "None", "NaT", ""]), c] = None

    # material wajib ada
    df = df[df["material"].notna()].copy()
    df["material"] = df["material"].astype(str).str.strip()

    return df

def get_existing_materials_set() -> set:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT material FROM zcorin_converter")).fetchall()
    return set(r[0] for r in rows if r and r[0] is not None)

st.subheader("Bulk Upload Excel")

uploaded = st.file_uploader("Upload your Master Data(.xlsx)", type=["xlsx"])

if uploaded:
    try:
        df_up = excel_to_db_df(uploaded)

        st.write("Preview (top 20):")
        st.dataframe(df_up.head(20), use_container_width=True)
        
        total_rows = len(df_up)
        st.caption(f"Total rows in file: {total_rows:,}")
        
        if st.button("Upload to DB (Insert New & Update Existing)"):
            with st.spinner("Analyzing data differences..."):
                uploaded_materials = df_up["material"].unique().tolist()
                
                # 2. Ambil data existing dari DB untuk material yang ada di Excel saja
                #    (Menggunakan tuple untuk query IN)
                if not uploaded_materials:
                    st.warning("File Excel kosong atau tidak ada kolom material.")
                    st.stop()
                    
                existing_map = {}
                
                # Batch fetch jika datanya banyak (chunking) agar tidak error query too long
                chunk_size = 1000
                
                for i in range(0, len(uploaded_materials), chunk_size):
                    chunk = uploaded_materials[i:i + chunk_size]
                    if not chunk: continue
                    
                    # Convert list to tuple string for SQL IN clause
                    # Handle single item tuple syntax issues
                    if len(chunk) == 1:
                        ids_str = f"('{chunk[0]}')"
                    else:
                        ids_str = str(tuple(chunk))
                    
                    sql_fetch = text(f"SELECT * FROM zcorin_converter WHERE material IN {ids_str}")
                    
                    with engine.connect() as conn:
                        rows = conn.execute(sql_fetch).mappings().all()
                        for r in rows:
                            existing_map[r['material']] = dict(r)
                
                to_insert = []
                to_update = []
                skipped_count = 0
                
                check_cols = [c for c in DB_COLS if c != 'material']
                
                for idx, row in df_up.iterrows():
                    mat = row['material']
                    # Ubah row pandas jadi dict bersih
                    row_dict = row.to_dict()

                    if mat not in existing_map:
                        # CASE 1: Material Belum Ada -> INSERT
                        to_insert.append(row_dict)
                    else:
                        # CASE 2: Material Sudah Ada -> CEK PERBEDAAN
                        db_row = existing_map[mat]
                        is_different = False
                        
                        for col in check_cols:
                            # Normalisasi value excel vs db untuk perbandingan
                            val_excel = row_dict.get(col)
                            val_db = db_row.get(col)

                            # Handle None/String mismatch
                            str_excel = str(val_excel).strip() if val_excel is not None else ""
                            str_db = str(val_db).strip() if val_db is not None else ""
                            
                            if str_excel != str_db:
                                is_different = True
                                break # Ada 1 beda cukup untuk trigger update
                        
                        if is_different:
                            to_update.append(row_dict)
                        else:
                            skipped_count += 1
                            
            msg_insert = ""
            msg_update = ""   
            
            if to_insert:
                insert_sql = text("""
                    INSERT INTO zcorin_converter
                    (material, material_description, country, brand, sub_brand, category, big_category, house, size,
                     pcs_cb, kg_cb, pack_format, size_format, insource_or_outsource, machine_1, updated_at)
                    VALUES
                    (:material, :material_description, :country, :brand, :sub_brand, :category, :big_category, :house, :size,
                     :pcs_cb, :kg_cb, :pack_format, :size_format, :insource_or_outsource, :machine_1, NOW())
                """)
                try:
                    with engine.begin() as conn:
                        conn.execute(insert_sql, to_insert)
                    msg_insert = f"✅ Sukses Insert: {len(to_insert)} data baru."
                except Exception as e:
                    st.error(f"Error Insert: {e}")

            # Eksekusi Update
            if to_update:
                update_sql = text("""
                    UPDATE zcorin_converter
                    SET 
                        material_description = :material_description,
                        country = :country,
                        brand = :brand,
                        sub_brand = :sub_brand,
                        category = :category,
                        big_category = :big_category,
                        house = :house,
                        size = :size,
                        pcs_cb = :pcs_cb,
                        kg_cb = :kg_cb,
                        pack_format = :pack_format,
                        size_format = :size_format,
                        insource_or_outsource = :insource_or_outsource,
                        machine_1 = :machine_1,
                        updated_at = NOW()
                    WHERE material = :material
                """)
                try:
                    with engine.begin() as conn:
                        conn.execute(update_sql, to_update)
                    msg_update = f"✏️ Sukses Update: {len(to_update)} data yang berubah."
                except Exception as e:
                    st.error(f"Error Update: {e}")
                    
            st.success("Upload selesai.")
            if msg_insert: st.write(msg_insert)
            if msg_update: st.write(msg_update)
            if skipped_count > 0:
                st.info(f"Skipped: {skipped_count} data (karena tidak ada perubahan).")
            
            # Refresh halaman agar tabel terupdate
            if to_insert or to_update:
                import time
                time.sleep(1)
                st.rerun()
        
    except Exception as e:
        st.error(f"Gagal memproses file: {e}")
else:
    st.caption("Upload Excel file for bulk upload.")

st.markdown("---")
st.subheader("Database Preview")
df_db = load_db(limit=5000)
st.dataframe(df_db, use_container_width=True)

st.sidebar.subheader("⚠️ Danger Zone")
confirm = st.sidebar.checkbox("Yes, I want to delete all data (TRUNCATE).")
if st.sidebar.button("TRUNCATE (clear all data)") and confirm:
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE zcorin_converter RESTART IDENTITY CASCADE"))
        st.sidebar.success("Table cleared.")
        st.rerun()
    except Exception as e:
        st.sidebar.error(f"Failed to truncate table: {e}")
