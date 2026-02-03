import io
import os
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Good Issue Cleaner", layout="wide")
st.title("Good Issue Cleaner")


uploaded = st.file_uploader("Upload file Good Issue (.xlsx)", type=["xlsx"])

if uploaded:
    # Get sheet names
    xls = pd.ExcelFile(uploaded, engine="openpyxl")
    sheet_names = xls.sheet_names

    selected_sheet = st.selectbox(
        "Pilih sheet yang akan diproses:",
        options=sheet_names,
        help="Pilih sheet dari file yang diupload"
    )

    if st.button("Start process Good Issue"):
        with st.spinner("Processing..."):
            df = pd.read_excel(uploaded, sheet_name=selected_sheet, usecols="H:J", engine="openpyxl")
            df.columns = ["Material", "Description", "Total Delivery quantity"]

            df["Total Delivery quantity"] = pd.to_numeric(df["Total Delivery quantity"], errors="coerce").fillna(0)

            result = (
                df.groupby("Material", as_index=False)
                .agg({
                    "Description": "first",
                    "Total Delivery quantity": "sum"
                })
            )

        st.markdown("---")

        st.subheader("Preview Output")
        st.dataframe(result, use_container_width=True)

        output = io.BytesIO()
        base_name = os.path.splitext(uploaded.name)[0]
        out_name = f"{base_name} Output.xlsx"

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            result.to_excel(writer, index=False, sheet_name="vis")
        output.seek(0)

        st.download_button(
            "Download Output (Excel)",
            data=output,
            file_name=out_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.caption("Upload your file to start the process.")
