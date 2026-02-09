import io
import pandas as pd
import streamlit as st
import datetime

st.set_page_config(page_title="ROFO Compiler", layout="wide")
st.title("ROFO Compiler")

tab1, tab2 = st.tabs(["Converter (Local/Export)", "Combined Mode"])

with tab1:
    uploaded_files = st.file_uploader(
        "Upload file SOP (.xlsx/.xlsb)",
        type=["xlsx", "xlsb"],
        accept_multiple_files=True,
        key="uploader_converter"
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

    month_names = ["January","February","March","April","May","June","July","August","September","October","November","December"]

    def add_months(y, m, add):
        total = (y * 12 + (m - 1)) + add
        return total // 12, (total % 12) + 1

    def format_cycle(val):
        try:
            if pd.isna(val): return val
            if isinstance(val, (int, float)):
                dt = pd.to_datetime(val, unit='D', origin='1899-12-30')
            else:
                dt = pd.to_datetime(val)
            return dt.strftime('%b-%y')
        except:
            return val

    def find_sku_col(df: pd.DataFrame) -> str:
        for c in df.columns:
            if "SKU" in str(c).upper(): return c
        raise KeyError("Kolom SKU Code tidak ditemukan.")

    def read_filtered(excel_file, sheet_name: str, year_filter: int) -> pd.DataFrame:
        fname = excel_file.name.lower() if hasattr(excel_file, 'name') else ""
        engine = "pyxlsb" if fname.endswith(".xlsb") else "openpyxl"
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=1, engine=engine)
            df = df.loc[:, ~df.columns.isna()]
            df = df.drop(columns=[c for c in df.columns if str(c).startswith("Unnamed")], errors="ignore")
            if "CYCLE" in df.columns:
                df["CYCLE"] = df["CYCLE"].apply(format_cycle)
            df = df[
                (df["DISTRIBUTOR"].astype(str).str.strip().str.upper() == FILTER_DISTRIBUTOR) &
                (df["UoM"].astype(str).str.strip().str.upper() == FILTER_UOM) &
                (pd.to_numeric(df["YEAR"], errors='coerce').fillna(-1).astype(int) == year_filter)
            ].copy()
            return df
        except:
            return pd.DataFrame()

    def process_sheet_multi(files, sheet_name: str, b_year: int, b_month: int) -> pd.DataFrame:
        base_df = None
        for f in files:
            tmp = read_filtered(f, sheet_name, b_year)
            if not tmp.empty:
                base_df = tmp
                break
        if base_df is None or base_df.empty: return pd.DataFrame()
        sku_col = find_sku_col(base_df)
        out = base_df.drop(columns=["FY", "TON2CTN", "Cek "], errors="ignore").copy()
        for i in range(4):
            yi, mi = add_months(b_year, b_month, i)
            m_name = month_names[mi - 1]
            for f in files:
                tmp = read_filtered(f, sheet_name, yi)
                if not tmp.empty and m_name in tmp.columns:
                    sku_tmp = find_sku_col(tmp)
                    out = out.merge(tmp[[sku_tmp, m_name]].rename(columns={sku_tmp: sku_col, m_name: f"M{i}"}), on=sku_col, how="left")
                    break
            out[f"M{i}"] = pd.to_numeric(out[f"M{i}"], errors="coerce").fillna(0).round(0).astype("Int64")
        return out

    def process_export_rofo(files, b_year, b_month):
        targets = [add_months(b_year, b_month, i) for i in range(4)]
        all_dfs = []
        for f in files:
            try:
                df_raw = pd.read_excel(f, sheet_name="ROFO", header=None)
                h_row = df_raw.iloc[4].values
                sel_idx = []
                for ty, tm in targets:
                    for idx in range(76, 88):
                        try:
                            val = h_row[idx]
                            dt = pd.to_datetime(val, unit='D', origin='1899-12-30') if isinstance(val, (int, float)) else pd.to_datetime(val)
                            if dt.year == ty and dt.month == tm:
                                sel_idx.append(idx)
                                break
                        except: continue
                sku_mask = pd.to_numeric(df_raw.iloc[5:, 1], errors='coerce').notna()
                data = df_raw.iloc[5:][sku_mask].copy()
                year_s = pd.Series([b_year]*len(data), index=data.index)
                uom_s = pd.Series(["Carton"]*len(data), index=data.index)
                res = pd.concat([year_s, data.iloc[:, 1], data.iloc[:, 2], data.iloc[:, 9], uom_s, data.iloc[:, sel_idx]], axis=1)
                res.columns = ["YEAR", "SKU CODE", "SKU DESCRIPTION", "DISTRIBUTOR", "UoM"] + [f"M{i}" for i in range(len(sel_idx))]
                for col in [f"M{i}" for i in range(len(sel_idx))]:
                    res[col] = pd.to_numeric(res[col], errors='coerce').fillna(0).round(0).astype("Int64")
                all_dfs.append(res)
            except: continue
        return pd.concat(all_dfs).drop_duplicates(subset=["SKU CODE"]) if all_dfs else pd.DataFrame()

    can_process = bool(uploaded_files)
    if st.button("🚀 Start Process", disabled=not can_process):
        with st.spinner("Processing..."):
            if rofo_type == "Local":
                ps = process_sheet_multi(uploaded_files, "PS_DRY", base_year, base_month)
                ss = process_sheet_multi(uploaded_files, "SS_DRY", base_year, base_month)
                st.success("Selesai (Local Mode)!")
                st.dataframe(ps, use_container_width=True)
                st.dataframe(ss, use_container_width=True)
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    ps.to_excel(writer, sheet_name="PS_DRY", index=False)
                    ss.to_excel(writer, sheet_name="SS_DRY", index=False)
                st.download_button("📥 Download Local ROFO", output.getvalue(), f"ROFO_Local_{base_year}.xlsx")
            else:
                export_df = process_export_rofo(uploaded_files, base_year, base_month)
                st.success("Selesai (Export Mode)!")
                st.dataframe(export_df, use_container_width=True)
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    export_df.to_excel(writer, sheet_name="ROFO_Export", index=False)
                st.download_button("📥 Download Export ROFO", output.getvalue(), f"ROFO_Export_{base_year}.xlsx")

with tab2:
    st.header("Combined File")
    
    c_up1, c_up2 = st.columns(2)
    with c_up1: file_local = st.file_uploader("Local file after convert", type=["xlsx"], key="comb_local")
    with c_up2: file_export = st.file_uploader("Export file after convert", type=["xlsx"], key="comb_export")
    
    if file_local and file_export:
        if st.button("Combine Data"):
            with st.spinner("Combining files..."):
                try:
                    df_local_ps = pd.read_excel(file_local, sheet_name="PS_DRY")
                    df_local_ss = pd.read_excel(file_local, sheet_name="SS_DRY")
                except:
                    xl = pd.ExcelFile(file_local)
                    df_local_ps = pd.read_excel(file_local, sheet_name=0)
                    df_local_ss = pd.read_excel(file_local, sheet_name=1) if len(xl.sheet_names) > 1 else pd.DataFrame()

                df_exp_source = pd.read_excel(file_export)
                df_exp_sync = df_exp_source.rename(columns={
                    "Year": "YEAR", 
                    "SKU Code": "SKU CODE", 
                    "SKU Description": "SKU DESCRIPTION", 
                    "Distributor": "DISTRIBUTOR",
                    "UoM": "UoM"
                })

                final_ps_export = pd.concat([df_local_ps, df_exp_sync], ignore_index=True, sort=False)
                
                st.success("Successfully Combined!")
                st.write("**Preview Combined PS & Export**")
                st.dataframe(final_ps_export.head(), use_container_width=True)
                
                # 4. Save to Excel with separate sheets
                out_comb = io.BytesIO()
                with pd.ExcelWriter(out_comb, engine="openpyxl") as writer:
                    final_ps_export.to_excel(writer, index=False, sheet_name="Combined_PS_Export")
                    if not df_local_ss.empty:
                        df_local_ss.to_excel(writer, index=False, sheet_name="Secondary_Sales_Local")
                
                st.download_button(
                    "📥 Download Combined ROFO", 
                    out_comb.getvalue(), 
                    "ROFO_Combined_Final.xlsx"
                )