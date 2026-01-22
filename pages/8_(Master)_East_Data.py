import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# =========================
# PAGE
# =========================
st.set_page_config(page_title="Master Data (East)", layout="wide")
st.title("Master Data (East)")

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

TABLE = "master_dps_east"

DB_COLS = ["material", "description", "size", "pcs_cb", "kg_cb", "line", "speed"]

EXCEL_MAPPING = {
    "Material": "material",
    "Description": "description",
    "Size": "size",
    "pcs/cb": "pcs_cb",
    "kg/cb": "kg_cb",
    "Line": "line",
    "Speed": "speed",
}

# =========================
# HELPERS
# =========================
def load_db(limit: int = 5000) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(
            text(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT :lim"),
            conn,
            params={"lim": limit},
        )

def get_row_by_material_and_line(material: str, line: str):
    """Get a specific row by material AND line combination."""
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT * FROM {TABLE} WHERE material = :m AND line = :l LIMIT 1"),
            {"m": material, "l": line},
        ).mappings().first()
    return dict(row) if row else None

def get_lines_for_material(material: str) -> list:
    """Get all lines associated with a material."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT DISTINCT line FROM {TABLE} WHERE material = :m ORDER BY line"),
            {"m": material},
        ).fetchall()
    return [r[0] for r in rows if r and r[0] is not None]

def get_all_lines() -> list:
    """Get all distinct lines from DB."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT DISTINCT line FROM {TABLE} WHERE line IS NOT NULL ORDER BY line")
        ).fetchall()
    return [r[0] for r in rows if r and r[0] is not None]

def get_all_materials() -> list:
    """Get all distinct materials from DB."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT DISTINCT material FROM {TABLE} WHERE material IS NOT NULL ORDER BY material")
        ).fetchall()
    return [r[0] for r in rows if r and r[0] is not None]

def insert_row(payload: dict):
    sql = text(f"""
        INSERT INTO {TABLE}
        (material, description, size, pcs_cb, kg_cb, line, speed, updated_at)
        VALUES
        (:material, :description, :size, :pcs_cb, :kg_cb, :line, :speed, NOW())
    """)
    with engine.begin() as conn:
        conn.execute(sql, payload)

def update_only_changed(material: str, line: str, old_row: dict, new_values: dict):
    """Update row identified by material + line, only changed columns."""
    changed = {}
    for k in DB_COLS:
        if k in ["material", "line"]:  # Skip key columns
            continue

        old_v = old_row.get(k)
        new_v = new_values.get(k)

        # Normalize string compare
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
        UPDATE {TABLE}
        SET {", ".join(set_parts)}, updated_at = NOW()
        WHERE material = :material AND line = :line
    """)

    params = {"material": material, "line": line, **changed}
    with engine.begin() as conn:
        res = conn.execute(sql, params)

    return res.rowcount, list(changed.keys())

def check_material_line_exists(material: str, line: str) -> bool:
    """Check if material + line combination exists."""
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT 1 FROM {TABLE} WHERE material = :m AND line = :l LIMIT 1"),
            {"m": material, "l": line},
        ).first()
    return row is not None

def get_existing_material_line_set() -> set:
    """Get set of (material, line) tuples for duplicate checking."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT material, line FROM {TABLE}")).fetchall()
    return set((r[0], r[1]) for r in rows if r and r[0] is not None and r[1] is not None)

def excel_to_db_df(uploaded) -> pd.DataFrame:
    df = pd.read_excel(uploaded, sheet_name="cleandata", engine="openpyxl")

    missing = [c for c in EXCEL_MAPPING.keys() if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in Excel sheet 'cleandata': {missing}")

    df = df.rename(columns=EXCEL_MAPPING)
    df = df[list(EXCEL_MAPPING.values())].copy()

    # material mandatory
    df["material"] = df["material"].astype(str).str.strip()
    df = df[df["material"].notna() & (df["material"] != "")].copy()

    # clean strings
    for c in ["description", "line"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df.loc[df[c].isin(["nan", "None", "NaT", ""]), c] = None

    # numeric cols
    for c in ["size", "pcs_cb", "kg_cb", "speed"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

mode = st.radio(
    "Select Mode",
    options=["✏️ Edit Existing", "➕ Add New"],
    horizontal=True,
    help="Switch between editing existing records or adding new ones"
)

st.markdown("---")

# =========================
# EDIT MODE
# =========================
if mode == "✏️ Edit Existing":
    st.markdown("### Search & Edit")
    
    # Get all materials for dropdown
    all_materials = get_all_materials()
    
    col_mat, col_line = st.columns(2)
    
    with col_mat:
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
    
    with col_line:
        if search_material:
            existing_lines = get_lines_for_material(search_material)
            if existing_lines:
                search_line = st.selectbox(
                    "Select Line",
                    options=existing_lines,
                    index=0,
                    help="Select a line to edit"
                )
            else:
                st.warning("No lines found for this material.")
                search_line = None
        else:
            st.selectbox("Select Line", options=["-- Select Material First --"], disabled=True)
            search_line = None
    
    # Show edit form if both are selected
    if search_material and search_line:
        row = get_row_by_material_and_line(search_material, search_line)
        
        if row:
            st.success(f"Editing: Material **{search_material}** on Line **{search_line}**")
            st.caption(f"DB id: {row.get('id')} | last updated: {row.get('updated_at')}")
            
            # Show other lines for this material
            all_lines_for_mat = get_lines_for_material(search_material)
            if len(all_lines_for_mat) > 1:
                other_lines = [l for l in all_lines_for_mat if l != search_line]
                st.info(f"📋 This material also exists on lines: **{', '.join(other_lines)}**")

            with st.form("edit_existing"):
                col1, col2 = st.columns(2)
                with col1:
                    st.text_input("Material *", value=row.get("material"), disabled=True)
                    st.text_input("Line *", value=row.get("line"), disabled=True)
                    description = st.text_input("Description", value=row.get("description") or "")
                with col2:
                    size = st.number_input("Size", value=float(row.get("size") or 0), step=1.0)
                    pcs_cb = st.number_input("pcs/cb", value=float(row.get("pcs_cb") or 0), step=1.0)
                    kg_cb = st.number_input("kg/cb", value=float(row.get("kg_cb") or 0), step=0.1)
                    speed = st.number_input("Speed", value=float(row.get("speed") or 0), step=0.1)

                submitted = st.form_submit_button("💾 Update (only changed)", use_container_width=True)
                if submitted:
                    new_values = {
                        "material": row["material"],
                        "description": description.strip() or None,
                        "size": size if size != 0 else None,
                        "pcs_cb": pcs_cb if pcs_cb != 0 else None,
                        "kg_cb": kg_cb if kg_cb != 0 else None,
                        "line": row["line"],
                        "speed": speed if speed != 0 else None,
                    }

                    count, changed_cols = update_only_changed(row["material"], row["line"], row, new_values)
                    if count == 0:
                        st.info("No changes detected. Nothing updated.")
                    else:
                        st.success(f"✅ Updated. Changed columns: {changed_cols}")
                    st.rerun()

# =========================
# ADD MODE
# =========================
else:  # Add New mode
    st.markdown("### ➕ Add New Record")
    
    # Get all lines for dropdown
    all_lines = get_all_lines()
    
    col_mat, col_line = st.columns(2)
    
    with col_mat:
        new_material = st.text_input(
            "Material *",
            placeholder="e.g. 163659",
            help="Enter the material code"
        ).strip()
    
    with col_line:
        col_line_select, col_line_custom = st.columns(2)
        with col_line_select:
            if all_lines:
                selected_line = st.selectbox(
                    "Select Line",
                    options=["-- Select --"] + all_lines,
                    index=0,
                    help="Select an existing line"
                )
            else:
                selected_line = "-- Select --"
                st.info("No lines in DB yet")
        with col_line_custom:
            custom_line = st.text_input(
                "Or New Line",
                placeholder="e.g. L5",
                help="Enter a new line name"
            ).strip()
    
    # Determine which line to use
    final_line = custom_line if custom_line else (selected_line if selected_line != "-- Select --" else None)
    
    # Validation and form
    if new_material and final_line:
        # Check if combination already exists
        if check_material_line_exists(new_material, final_line):
            st.error(f"❌ Material **{new_material}** with Line **{final_line}** already exists! Use Edit mode to modify.")
        else:
            # Check if material exists with other lines
            existing_lines = get_lines_for_material(new_material)
            if existing_lines:
                st.info(f"ℹ️ Material **{new_material}** exists with lines: **{', '.join(existing_lines)}**. Adding new line **{final_line}**.")
            else:
                st.success(f"✅ Ready to add new Material **{new_material}** on Line **{final_line}**")
            
            with st.form("add_new"):
                col1, col2 = st.columns(2)
                with col1:
                    st.text_input("Material *", value=new_material, disabled=True)
                    st.text_input("Line *", value=final_line, disabled=True)
                    description = st.text_input("Description", placeholder="Product description")
                with col2:
                    size = st.number_input("Size", value=0.0, step=1.0)
                    pcs_cb = st.number_input("pcs/cb", value=0.0, step=1.0)
                    kg_cb = st.number_input("kg/cb", value=0.0, step=0.1)
                    speed = st.number_input("Speed", value=0.0, step=0.1)

                submitted = st.form_submit_button("➕ Insert", use_container_width=True)
                if submitted:
                    payload = {
                        "material": new_material,
                        "description": description.strip() or None,
                        "size": size if size != 0 else None,
                        "pcs_cb": pcs_cb if pcs_cb != 0 else None,
                        "kg_cb": kg_cb if kg_cb != 0 else None,
                        "line": final_line,
                        "speed": speed if speed != 0 else None,
                    }
                    insert_row(payload)
                    st.success("✅ Inserted successfully!")
                    st.rerun()
    
    elif new_material and not final_line:
        st.warning("⚠️ Please select or enter a Line.")
    elif not new_material and final_line:
        st.warning("⚠️ Please enter a Material.")

st.markdown("---")

# =========================
# BULK UPLOAD (INSERT NEW ONLY)
# =========================
st.subheader("Bulk Upload (Insert New Only)")

uploaded = st.file_uploader("Upload Excel (.xlsx) containing sheet: cleandata", type=["xlsx"])

if uploaded:
    try:
        df_up = excel_to_db_df(uploaded)
        st.write("Preview (top 20):")
        st.dataframe(df_up.head(20), use_container_width=True)
        st.caption(f"Rows in file: {len(df_up):,}")

        if st.button("📥 Upload to DB"):
            existing = get_existing_material_line_set()

            # Create tuple for comparison
            df_up["_key"] = list(zip(df_up["material"], df_up["line"]))
            
            dup_keys = [k for k in df_up["_key"] if k in existing]
            df_new = df_up[~df_up["_key"].isin(existing)].copy()
            df_new = df_new.drop(columns=["_key"])

            if len(df_new) > 0:
                insert_sql = text(f"""
                    INSERT INTO {TABLE}
                    (material, description, size, pcs_cb, kg_cb, line, speed, updated_at)
                    VALUES
                    (:material, :description, :size, :pcs_cb, :kg_cb, :line, :speed, NOW())
                """)
                with engine.begin() as conn:
                    conn.execute(insert_sql, df_new.to_dict(orient="records"))

            st.success(f"✅ Inserted new rows: {len(df_new):,}")
            if dup_keys:
                st.warning(f"⚠️ Skipped duplicates (Material+Line already in DB): {len(dup_keys):,}")
                with st.expander("Show skipped duplicates"):
                    st.write([f"{m} | {l}" for m, l in dup_keys[:50]])

            st.cache_resource.clear()

    except Exception as e:
        st.error(f"Failed to process file: {e}")
else:
    st.caption("Upload Excel file to bulk insert new materials.")

st.markdown("---")

# =========================
# DB PREVIEW
# =========================
st.subheader("Database Preview")
df_db = load_db(limit=5000)
st.dataframe(df_db, use_container_width=True)
st.caption(f"Showing {len(df_db):,} rows")
