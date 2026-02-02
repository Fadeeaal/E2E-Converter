import io
import pandas as pd
import streamlit as st
import datetime

st.set_page_config(page_title="ROFO Compiler", layout="wide")
st.title("ROFO Compiler")

uploaded_files = st.file_uploader(
    "Upload file SOP (.xlsx/.xlsb)",
    type=["xlsx", "xlsb"],
    accept_multiple_files=True
)

c1, c2 = st.columns(2)
with c1:
    base_year = st.number_input("Tahun M0", min_value=2000, max_value=2100, value=2026, step=1)
with c2:
    base_month = st.number_input("Bulan M0 (1-12)", min_value=1, max_value=12, value=1, step=1)

rofo_type = st.radio(
    "Select Type", 
    ["Local", "Export"], 
    horizontal=True, 
    help="Pilih 'Local' untuk sheet PS_DRY/SS_DRY, atau 'Export' untuk sheet ROFO."
)

FILTER_DISTRIBUTOR = "NATIONAL"
FILTER_UOM = "CARTON"

if rofo_type == "Local":
    d1, d2 = st.columns(2)
    with d1:
        st.text_input("Distributor", value=FILTER_DISTRIBUTOR, disabled=True)
    with d2:
        st.text_input("UoM", value=FILTER_UOM, disabled=True)

month_names = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
]

def add_months(y, m, add):
    total = (y * 12 + (m - 1)) + add
    ny = total // 12
    nm = (total % 12) + 1
    return ny, nm

def find_sku_col(df: pd.DataFrame) -> str:
    candidates = ["SKU CODE"]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "SKU" in str(c).upper():
            return c
    raise KeyError("Kolom SKU Code tidak ditemukan di sheet.")

def read_filtered(excel_file, sheet_name: str, year_filter: int) -> pd.DataFrame:
    fname = excel_file.name.lower() if hasattr(excel_file, 'name') else ""
    engine = "pyxlsb" if fname.endswith(".xlsb") else "openpyxl"
    
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=1, engine=engine)
        df = df.loc[:, ~df.columns.isna()]
        df = df.drop(columns=[c for c in df.columns if str(c).startswith("Unnamed")], errors="ignore")

        df = df[
            (df["DISTRIBUTOR"].astype(str).str.strip().str.upper() == FILTER_DISTRIBUTOR) &
            (df["UoM"].astype(str).str.strip().str.upper() == FILTER_UOM) &
            (df["YEAR"].fillna(-1).astype(int) == year_filter)
        ].copy()
        return df
    except:
        return pd.DataFrame()

def process_sheet_multi(files, sheet_name: str, base_year: int, base_month: int) -> pd.DataFrame:
    base_df = None
    for f in files:
        tmp = read_filtered(f, sheet_name, base_year)
        if not tmp.empty:
            base_df = tmp
            break

    if base_df is None or base_df.empty:
        return pd.DataFrame()

    drop_cols = ["Magnitude PH L1 Code", "Magnitude PH L1 Description", "Magnitude PH L2 Code", 
                 "Magnitude PH L2 Description", "Magnitude PH L4 Code", "Magnitude PH L4 Description",
                 "RF Product Group L1", "FY", "Cek ", "Cek .1", "TON2CTN"]
    base_df = base_df.drop(columns=drop_cols, errors="ignore")

    sku_col = find_sku_col(base_df)
    meta_cols = [c for c in base_df.columns if c not in month_names]
    out = base_df[meta_cols].copy()

    for i in range(4):
        yi, mi = add_months(base_year, base_month, i)
        month_col = month_names[mi - 1]
        found = None
        for f in files:
            tmp = read_filtered(f, sheet_name, yi)
            if not tmp.empty and month_col in tmp.columns:
                sku_tmp = find_sku_col(tmp)
                tmp2 = tmp[[sku_tmp, month_col]].copy()
                tmp2 = tmp2.rename(columns={sku_tmp: sku_col, month_col: f"M{i}"})
                found = tmp2
                break
        if found is not None:
            out = out.merge(found, on=sku_col, how="left")
        out[f"M{i}"] = pd.to_numeric(out[f"M{i}"], errors="coerce").fillna(0).round(0).astype("Int64")
    return out

def process_export_rofo(files, base_year, base_month):
    targets = [add_months(base_year, base_month, i) for i in range(4)]
    all_dfs = []
    
    for f in files:
        fname = f.name.lower() if hasattr(f, 'name') else ""
        engine = "pyxlsb" if fname.endswith(".xlsb") else "openpyxl"
        
        try:
            df_raw = pd.read_excel(f, sheet_name="ROFO", header=None, engine=engine)
            header_row = df_raw.iloc[4].values

            month_col_indices = list(range(76, 88))
            
            selected_indices = []
            for ty, tm in targets:
                for idx in month_col_indices:
                    try:
                        dt = pd.to_datetime(header_row[idx])
                        if dt.year == ty and dt.month == tm:
                            selected_indices.append(idx)
                            break
                    except: continue

            sku_mask = pd.to_numeric(df_raw.iloc[5:, 1], errors='coerce').notna()
            data_rows = df_raw.iloc[5:][sku_mask].copy()
            
            year_col = pd.Series([base_year] * len(data_rows), index=data_rows.index)

            dist_col = data_rows.iloc[:, 9]
            
            res_df = pd.concat([
                year_col,          
                data_rows.iloc[:, 1], 
                data_rows.iloc[:, 2],
                dist_col,
                data_rows.iloc[:, selected_indices]
            ], axis=1)
            
            # Beri nama kolom yang rapi
            cols = ["Year", "SKU Code", "SKU Description", "Distributor"] + [f"M{i}" for i in range(len(selected_indices))]
            res_df.columns = cols
            
            # Cleaning data numerik
            for col in [f"M{i}" for i in range(len(selected_indices))]:
                res_df[col] = pd.to_numeric(res_df[col], errors='coerce').fillna(0).round(0).astype("Int64")
            
            all_dfs.append(res_df)
        except Exception as e:
            st.warning(f"Gagal memproses file {fname}: {e}")
            
    return pd.concat(all_dfs).drop_duplicates(subset=["SKU Code"]) if all_dfs else pd.DataFrame()

if uploaded_files:
    if st.button("🚀 Start Process"):
        with st.spinner("Processing data..."):
            if rofo_type == "Local":
                ps = process_sheet_multi(uploaded_files, "PS_DRY", base_year, base_month)
                ss = process_sheet_multi(uploaded_files, "SS_DRY", base_year, base_month)
                
                if ps.empty and ss.empty:
                    st.error("Data tidak ditemukan di sheet PS_DRY atau SS_DRY.")
                else:
                    st.success("Selesai (Local Mode)!")
                    st.subheader("PS DRY")
                    st.dataframe(ps, use_container_width=True)
                    st.subheader("SS DRY")
                    st.dataframe(ss, use_container_width=True)
                    
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine="openpyxl") as writer:
                        ps.to_excel(writer, sheet_name="PS_DRY", index=False)
                        ss.to_excel(writer, sheet_name="SS_DRY", index=False)
                    st.download_button("📥 Download Local ROFO", output.getvalue(), f"ROFO_Local_{base_year}.xlsx")
            
            else:
                export_df = process_export_rofo(uploaded_files, base_year, base_month)
                if export_df.empty:
                    st.error("Data ROFO Export tidak ditemukan atau kolom M0-M3 tidak cocok.")
                else:
                    st.success("Selesai (Export Mode)!")
                    st.dataframe(export_df, use_container_width=True)
                    
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine="openpyxl") as writer:
                        export_df.to_excel(writer, sheet_name="ROFO_Export", index=False)
                    st.download_button("📥 Download Export ROFO", output.getvalue(), f"ROFO_Export_{base_year}.xlsx")
else:
    st.info("Silakan upload file SOP untuk memulai.")