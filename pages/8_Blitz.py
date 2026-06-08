import streamlit as st
import pandas as pd
import io
from datetime import datetime

st.set_page_config(page_title="Blitz Data Aggregator", layout="wide")
st.title("Blitz Converter")

st.subheader("Period")
c1, c2 = st.columns(2)

with c1:
    target_month = st.selectbox(
        "Pilih Bulan",
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        index=3
    )

with c2:
    target_year = st.text_input("Masukkan Tahun", value="2026")

target_period = f"{target_month} {target_year}"

st.subheader("Upload your file")
uploaded_file = st.file_uploader(
    "Upload File Blitz (.xlsx/.csv)",
    type=["xlsx", "csv"]
)

if uploaded_file:
    # ── PILIH SHEET UNTUK FILE EXCEL
    if uploaded_file.name.endswith(".csv"):
        df_raw = pd.read_csv(uploaded_file, header=None)
        selected_sheet = "CSV File"
    else:
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = xls.sheet_names

        selected_sheet = st.selectbox(
            "Pilih Sheet yang ingin diproses",
            sheet_names
        )

        df_raw = pd.read_excel(
            uploaded_file,
            sheet_name=selected_sheet,
            header=None
        )

    if st.button("Start Processing"):
        with st.spinner(f"Processing {target_period}'s data from sheet '{selected_sheet}'..."):

            # ── 1. Cari start_row
            start_row = None
            for r in range(len(df_raw)):
                row_values = df_raw.iloc[r].astype(str).str.strip().tolist()

                if "SKU Ori ID" in row_values:
                    start_row = r
                    break

            if start_row is None:
                st.error("Header 'SKU Ori ID' tidak ditemukan.")
                st.stop()

            header_list = df_raw.iloc[start_row].astype(str).str.strip().tolist()

            # ── 2. Cari kolom periode
            col_stock = None
            col_ss = None

            month_list = [
                "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
            ]

            for r_search in range(max(0, start_row - 5), start_row + 1):
                for c_idx, cell_val in enumerate(df_raw.iloc[r_search].tolist()):

                    if pd.notna(cell_val) and target_period.lower() in str(cell_val).lower():

                        for c_search in range(c_idx, len(header_list)):
                            h = str(header_list[c_search]).strip()

                            if "Total Total" in h:
                                break

                            if "Total Stock in CTN" in h and col_stock is None:
                                col_stock = c_search

                            if "Total SS in CTN" in h and col_ss is None:
                                col_ss = c_search

                            if col_stock is not None and col_ss is not None:
                                break

                            period_cell = str(df_raw.iloc[r_search, c_search])

                            if (
                                c_search > c_idx
                                and any(m in period_cell for m in month_list)
                                and period_cell.lower() != target_period.lower()
                            ):
                                break

                if col_stock is not None and col_ss is not None:
                    break

            if col_stock is None or col_ss is None:
                st.error(f"Kolom untuk '{target_period}' tidak ditemukan.")
                st.stop()

            # ── 3. Cari kolom ID via Distributor
            col_dist = None
            for ci, val in enumerate(header_list):
                if str(val).strip() == "Distributor Name":
                    col_dist = ci
                    break

            if col_dist is None:
                st.error("Kolom 'Distributor Name' tidak ditemukan.")
                st.stop()

            col_id = None
            for ci in range(col_dist - 1, -1, -1):
                if str(header_list[ci]).strip() == "SKU Ori ID":
                    col_id = ci
                    break

            if col_id is None:
                st.error("Tidak bisa menemukan 'SKU Ori ID'.")
                st.stop()

            # ── 4. Extract data
            raw_data = []

            for r in range(start_row + 1, len(df_raw)):
                row_vals = df_raw.iloc[r]

                row_str = " ".join(str(v) for v in row_vals.values if pd.notna(v))
                if "Grand Total" in row_str:
                    break

                sku_id = row_vals.iloc[col_id]

                if pd.isna(sku_id) or str(sku_id).strip() == "":
                    continue

                dist_val = row_vals.iloc[col_dist]

                if pd.isna(dist_val) or str(dist_val).strip() == "":
                    continue

                raw_data.append({
                    "SKU Ori ID": str(sku_id).strip(),
                    "Stock": row_vals.iloc[col_stock],
                    "SS": row_vals.iloc[col_ss]
                })

            df_extracted = pd.DataFrame(raw_data)

            if df_extracted.empty:
                st.error("Tidak ada data yang berhasil diekstrak.")
                st.stop()

            df_extracted["Stock"] = pd.to_numeric(df_extracted["Stock"], errors="coerce").fillna(0)
            df_extracted["SS"] = pd.to_numeric(df_extracted["SS"], errors="coerce").fillna(0)

            # ── 5. Aggregation
            df_stock_final = (
                df_extracted.groupby("SKU Ori ID", as_index=False)["Stock"]
                .sum()
                .rename(columns={"Stock": "Sum of M0"})
            )

            df_ss_final = (
                df_extracted.groupby("SKU Ori ID", as_index=False)["SS"]
                .sum()
                .rename(columns={"SS": "Sum of M0"})
            )

            st.markdown("---")
            st.success(f"Data {target_period} berhasil ditarik dari sheet '{selected_sheet}'.")

            t1, t2 = st.tabs(["Opening Stock", "Actual SS"])

            with t1:
                st.dataframe(df_stock_final)

            with t2:
                st.dataframe(df_ss_final)

            # ── 6. Export
            output = io.BytesIO()

            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_stock_final.to_excel(writer, index=False, sheet_name="Total Stock")
                df_ss_final.to_excel(writer, index=False, sheet_name="Total SS")

            st.download_button(
                label="Download Hasil (.xlsx)",
                data=output.getvalue(),
                file_name=f"{datetime.now().strftime('%d%m%y')}_Blitz Data_{target_period}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )