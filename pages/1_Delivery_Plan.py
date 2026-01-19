import io
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Delivery Plan Extractor", layout="wide")
st.title("Delivery Plan Extractor")

uploaded = st.file_uploader("Upload file Excel (.xlsx)", type=["xlsx"])

if uploaded:
    xls = pd.ExcelFile(uploaded, engine="openpyxl")
    sheet = st.selectbox("Pilih sheet", options=xls.sheet_names, index=0)

    if st.button("Process"):
        df = pd.read_excel(
            uploaded,
            sheet_name=sheet,
            header=4,
            usecols="B:D,BG:BK",
            engine="openpyxl"
        )

        # Filter: buang subheader & summary
        if "Demand Code" in df.columns:
            df = df[df["Demand Code"].notna()].copy()
        if "Description" in df.columns:
            df = df[df["Description"].notna()].copy()
            df["Description"] = df["Description"].astype(str).str.strip()

        # ====== ROUND kolom angka (week1 - total) ======
        non_numeric_cols = {"Demand Code", "Description"}
        numeric_cols = [c for c in df.columns if c not in non_numeric_cols]

        for c in numeric_cols:
            s = (
                df[c]
                .astype(str)
                .str.replace("-", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.strip()
            )
            df[c] = pd.to_numeric(s, errors="coerce").round(0).astype("Int64")

        st.success(f"Selesai. Rows: {len(df)}")
        st.dataframe(df, use_container_width=True)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Output")
        output.seek(0)

        st.download_button(
            label="Download hasil (Excel)",
            data=output,
            file_name="DeliveryPlan_Output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.caption("Upload your file to start the process.")
