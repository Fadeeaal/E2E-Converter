import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import datetime

st.set_page_config(page_title="Blitz Data Aggregator", layout="wide")
st.title("Blitz Converter")

# =========================
# STEP 1: INPUT PARAMETERS (MAIN PAGE)
# =========================
st.subheader("Period")
c1, c2 = st.columns(2)
with c1:
    target_month = st.selectbox("Pilih Bulan", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
with c2:
    target_year = st.text_input("Masukkan Tahun", value="2026")

target_period = f"{target_month} {target_year}" 

# =========================
# STEP 2: FILE UPLOAD
# =========================
st.subheader("Upload your file")
uploaded_file = st.file_uploader("Upload File Blitz (.xlsx/.csv)", type=["xlsx", "csv"])

if uploaded_file:
    # Membaca file mentah
    if uploaded_file.name.endswith('.csv'):
        df_raw = pd.read_csv(uploaded_file, header=None)
    else:
        df_raw = pd.read_excel(uploaded_file, header=None)

    if st.button("Start Processing"):
        with st.spinner(f"Processing {target_period}'s data..."):
            
            # --- Mencari Baris Header (SKU Ori ID) ---
            start_row = None
            for r in range(len(df_raw)):
                row_values = df_raw.iloc[r].astype(str).tolist()
                if "SKU Ori ID" in row_values:
                    start_row = r
                    break

            if start_row is not None:
                # --- Mencari Kolom Periode & Data ---
                col_stock, col_ss = None, None
                
                # Scan baris di atas header untuk mencari teks periode
                for r_search in range(max(0, start_row-5), start_row):
                    row_period = df_raw.iloc[r_search].tolist()
                    for c_idx, cell_val in enumerate(row_period):
                        if pd.notna(cell_val) and target_period.lower() in str(cell_val).lower():
                            row_headers = df_raw.iloc[start_row].astype(str).tolist()
                            # Cari kolom target di area periode tersebut
                            for c_search in range(c_idx, len(row_headers)):
                                if row_headers[c_search] == "Total Stock in CTN":
                                    col_stock = c_search
                                if row_headers[c_search] == "Total SS in CTN":
                                    col_ss = c_search
                                if col_stock is not None and col_ss is not None:
                                    break
                    if col_stock is not None: break

                if col_stock is not None and col_ss is not None:
                    # Identifikasi Kolom ID dan Name
                    row_headers_list = df_raw.iloc[start_row].astype(str).tolist()
                    col_id = row_headers_list.index("SKU Ori ID")
                    col_name = row_headers_list.index("SKU Ori Name")
                    
                    # --- Ekstraksi Data Mentah ---
                    raw_data = []
                    for r in range(start_row + 1, len(df_raw)):
                        row_vals = df_raw.iloc[r].values
                        # Berhenti jika Grand Total
                        if any("Grand Total" in str(x) for x in row_vals):
                            break
                        
                        raw_data.append({
                            "SKU Ori ID": df_raw.iloc[r, col_id],
                            "SKU Ori Name": df_raw.iloc[r, col_name],
                            "Stock": pd.to_numeric(df_raw.iloc[r, col_stock], errors='coerce'),
                            "SS": pd.to_numeric(df_raw.iloc[r, col_ss], errors='coerce')
                        })

                    df_extracted = pd.DataFrame(raw_data).dropna(subset=["SKU Ori ID"])
                    
                    # --- AGREGASI & LOGIKA PEMBULATAN ---
                    # Logic: Groupby Sum -> Ceil (Bulat ke atas) -> Clip 0 (Minus jadi 0)
                    
                    # Sheet Stock
                    df_stock_final = df_extracted.groupby(["SKU Ori ID", "SKU Ori Name"])["Stock"].sum().reset_index()
                    df_stock_final["Sum of M0"] = np.ceil(df_stock_final["Stock"]).clip(lower=0).astype(int)
                    df_stock_final = df_stock_final[["SKU Ori ID", "SKU Ori Name", "Sum of M0"]]
                    
                    # Sheet SS
                    df_ss_final = df_extracted.groupby(["SKU Ori ID", "SKU Ori Name"])["SS"].sum().reset_index()
                    df_ss_final["Sum of M0"] = np.ceil(df_ss_final["SS"]).clip(lower=0).astype(int)
                    df_ss_final = df_ss_final[["SKU Ori ID", "SKU Ori Name", "Sum of M0"]]

                    # --- HASIL AKHIR ---
                    st.markdown("---")
                    st.success(f"✅ Processing Complete!")
                    
                    t1, t2 = st.tabs(["Opening Stock Sheet", "Actual SS Sheet"])
                    with t1:
                        st.dataframe(df_stock_final, use_container_width=True)
                    with t2:
                        st.dataframe(df_ss_final, use_container_width=True)

                    today_str = datetime.now().strftime('%Y%m%d')
                    
                    custom_filename = f"{today_str}_BlitzData - {target_period}.xlsx"
                    
                    # Export ke Excel (2 Sheets)
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_stock_final.to_excel(writer, index=False, sheet_name="Total Stock")
                        df_ss_final.to_excel(writer, index=False, sheet_name="Total SS")
                    
                    st.download_button(
                        label="📥 Download Aggregated Excel",
                        data=output.getvalue(),
                        file_name=custom_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.error(f"'{target_period}' not found in the file. Please check the period and file format.")
            else:
                st.error("SKU Ori ID not found. Please check the file format.")
else:
    st.info("Upload your blitz file.")