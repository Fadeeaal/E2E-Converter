import io
import os
import pandas as pd
import streamlit as st
from datetime import datetime

def datenow_mmddyyyy():
    return datetime.now().strftime("%m%d%Y")

st.set_page_config(page_title="COOIS Cleaner", layout="wide")
st.title("COOIS Cleaner")

uploaded = st.file_uploader("Upload your COOIS file (.xlsx)", type=["xlsx"])

if uploaded:
    xls = pd.ExcelFile(uploaded, engine="openpyxl")
    sheet_names = xls.sheet_names

    selected_sheet = st.selectbox(
        "Pilih sheet yang akan diproses:",
        options=sheet_names,
        help="Pilih sheet dari file yang diupload"
    )

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Dari tanggal")
    with col2:
        end_date = st.date_input("Sampai tanggal")

    if st.button("Start cleaning COOIS data"):
        with st.spinner("Processing..."):
            df = pd.read_excel(
                uploaded,
                sheet_name=selected_sheet,
                usecols=[2, 3, 7, 9, 16],  # C, D, H, J, Q
                engine="openpyxl"
            )

            h_col = df.columns[2]  # H adalah indeks ke-2 (C=0, D=1, H=2, J=3, Q=4)
            df = df[df[h_col] == "TR"]
            df = df.drop(columns=[h_col]).reset_index(drop=True)
            # Setelah drop H: C=col[0], D=col[1], F=col[2], Q=col[3]

            c_col, d_col, f_col, q_col = df.columns[0], df.columns[1], df.columns[2], df.columns[3]

            df[q_col] = pd.to_datetime(df[q_col], errors="coerce")
            df = df[
                (df[q_col] >= pd.Timestamp(start_date)) &
                (df[q_col] <= pd.Timestamp(end_date))
            ]

            df[f_col] = pd.to_numeric(df[f_col], errors="coerce").fillna(0)

            result = (
                df.groupby([c_col, q_col], as_index=False)
                .agg({d_col: "first", f_col: "sum"})
                .sort_values(q_col)
                .reset_index(drop=True)
            )

            result[q_col] = result[q_col].dt.strftime("%d/%m/%Y")

            # Sheet RAW: semua kolom dari source, filter H=="TR" dan range tanggal
            df_raw = pd.read_excel(uploaded, sheet_name=selected_sheet, engine="openpyxl")
            raw_h_col = df_raw.columns[7]   # kolom H (0-indexed)
            raw_q_col = df_raw.columns[16]  # kolom Q (0-indexed)
            df_raw = df_raw[df_raw[raw_h_col] == "TR"]
            df_raw[raw_q_col] = pd.to_datetime(df_raw[raw_q_col], errors="coerce")
            df_raw = df_raw[
                (df_raw[raw_q_col] >= pd.Timestamp(start_date)) &
                (df_raw[raw_q_col] <= pd.Timestamp(end_date))
            ].reset_index(drop=True)
            df_raw[raw_q_col] = df_raw[raw_q_col].dt.strftime("%d/%m/%Y")

        st.markdown("---")

        st.subheader("Preview Output")
        tab_raw, tab_acc = st.tabs(["RAW", "ACCUMULATED"])
        with tab_raw:
            st.dataframe(df_raw, use_container_width=True)
        with tab_acc:
            st.dataframe(result, use_container_width=True)

        output = io.BytesIO()
        base_name = os.path.splitext(uploaded.name)[0]
        out_name = f"{base_name} Output.xlsx"

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_raw.to_excel(writer, index=False, sheet_name="RAW")
            result.to_excel(writer, index=False, sheet_name="ACCUMULATED")
        output.seek(0)

        st.download_button(
            "Download Output (Excel)",
            data=output,
            file_name=f"{datenow_mmddyyyy()}_{out_name}",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.caption("Upload your file to start the process.")