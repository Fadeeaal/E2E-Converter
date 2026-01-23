import io
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

st.set_page_config(page_title="ZCORIN Cleaner", layout="wide")
st.title("ZCORIN Cleaner")

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

def load_conversion_map():
    """material -> pcs_cb (numeric)"""
    sql = text("SELECT material, pcs_cb FROM zcorin_converter")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    df["material"] = df["material"].astype(str).str.strip()
    df["pcs_cb"] = pd.to_numeric(df["pcs_cb"], errors="coerce")
    return dict(zip(df["material"], df["pcs_cb"]))

def parse_date_series(s: pd.Series) -> pd.Series:
    """
    Convert date-like strings from SAP/Excel to real datetime.
    Primary guess: mm/dd/yyyy (common in exports),
    fallback: generic parse.
    """

    a = pd.to_datetime(s, format="%m/%d/%Y", errors="coerce")
    b = pd.to_datetime(s, errors="coerce", dayfirst=False)
    return a.combine_first(b)

uploaded = st.file_uploader("Upload file ZCORIN (.xlsx)", type=["xlsx"])
start_time = st.date_input("Start Time", value=None)

if not uploaded:
    st.caption("Upload your file to start the process.")

else :
    if not start_time:
        st.caption("Fill in Start Time first (date input).")
        st.stop()
    
    else :
        if st.button("Start process ZCORIN"):
            with st.spinner("Processing..."):
                df = pd.read_excel(uploaded, sheet_name="Sheet1", engine="openpyxl")
                
                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

                storage_col = df.columns[1]  
                unit_col = df.columns[12]    

                df_f = df[
                    (df[storage_col].isin([1, 6]) | df[storage_col].isna()) &
                    (df[unit_col].astype(str).str.strip().str.upper() == "PC")
                ].copy()

                def storage_sort_key(x):
                    if pd.isna(x) or str(x).strip() == "":
                        return 0
                    if str(x).strip() == "1":
                        return 1
                    if str(x).strip() == "6":
                        return 2
                    return 99

                df_f["_storage_sort"] = df_f[storage_col].apply(storage_sort_key)
                df_f = df_f.sort_values("_storage_sort").drop(columns="_storage_sort")

                required_cols = [
                    "Material",
                    "Unrestricted",
                    "Blocked",
                    "Qual. Inspection",
                    "Transfer",
                    "Returns(Blocked)",
                    "In Transit-Receivi",
                    "SLED/BBD",
                    "Manuf. Dte",
                ]
                missing = [c for c in required_cols if c not in df_f.columns]
                if missing:
                    st.error(f"Kolom ini tidak ditemukan di file: {missing}")
                    st.stop()


                df_f["SLED/BBD"] = parse_date_series(df_f["SLED/BBD"])
                df_f["Manuf. Dte"] = parse_date_series(df_f["Manuf. Dte"])
                df_f["Start Time"] = pd.to_datetime(start_time)


                conv_map = load_conversion_map()
                # Pastikan kolom Conversion ada sebelum dipakai
                df_f["Conversion"] = df_f["Material"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().map(conv_map)

                df_f["Unrestricted_vis"] = df_f["Unrestricted"] / df_f["Conversion"]
                df_f["Blocked_vis"] = df_f["Blocked"] / df_f["Conversion"]
                df_f["Qual. Inspection_vis"] = df_f["Qual. Inspection"] / df_f["Conversion"]
                df_f["Transfer_vis"] = df_f["Transfer"] / df_f["Conversion"]
                df_f["Returns(Blocked)_vis"] = df_f["Returns(Blocked)"] / df_f["Conversion"]
                df_f["In Transit-Receivi_vis"] = df_f["In Transit-Receivi"] / df_f["Conversion"]
                df_f["Total_vis"] = (
                    df_f["Unrestricted_vis"].fillna(0)
                    + df_f["Qual. Inspection_vis"].fillna(0)
                    + df_f["In Transit-Receivi_vis"].fillna(0)
                )
                df_f["Shelf Life"] = ((df_f["SLED/BBD"] - df_f["Start Time"]).dt.days / 360).round(2)
                df_f["Total Shelf life (years)"] = ((df_f["SLED/BBD"] - df_f["Manuf. Dte"]).dt.days / 360).round(2)
                df_f["Remaining Shelf life (%)"] = (
                    (df_f["Shelf Life"] / df_f["Total Shelf life (years)"] * 100).round(2)
                )
                df_f["Aging (month)"] = ((df_f["Start Time"] - df_f["Manuf. Dte"]).dt.days / 30).round(2)
                df_f["Unit_vis"] = "Ctn"
                df_f["MRP Controller_vis"] = ""
                df_f["Vendor Batch_vis"] = ""

                # Clean up inf/-inf and NaN
                for col in [
                    "Unrestricted_vis", "Blocked_vis", "Qual. Inspection_vis", "Transfer_vis",
                    "Returns(Blocked)_vis", "In Transit-Receivi_vis", "Total_vis",
                    "Shelf Life", "Total Shelf life (years)", "Remaining Shelf life (%)", "Aging (month)"
                ]:
                    df_f[col] = df_f[col].replace([float('inf'), float('-inf')], pd.NA)


                # Reorder columns as requested
                output_columns = [
                    "Plant", "Storage Location", "Material", "Material Description", "Batch", "SLED/BBD", "Manuf. Dte",
                    "Unrestricted", "Blocked", "Qual. Inspection", "Transfer", "Returns(Blocked)", "Unit", "MRP Controller", "Vendor Batch",
                    "In Transit-Receivi", "Start Time", "Conversion",
                    "Unrestricted_vis", "Blocked_vis", "Qual. Inspection_vis", "Transfer_vis", "Returns(Blocked)_vis",
                    "Unit_vis", "MRP Controller_vis", "Vendor Batch_vis", "In Transit-Receivi_vis", "Total_vis",
                    "Shelf Life", "Total Shelf life (years)", "Remaining Shelf life (%)", "Aging (month)"
                ]
                # Only keep columns that exist in df_f
                output_columns = [col for col in output_columns if col in df_f.columns]
                df_f = df_f[output_columns]

                base_name = os.path.splitext(uploaded.name)[0]
                out_name = f"{base_name}_vis.xlsx"

                out_bytes = io.BytesIO()
                with pd.ExcelWriter(out_bytes, engine="openpyxl") as writer:
                    df_f.to_excel(writer, index=False, sheet_name="Output")
                out_bytes.seek(0)

            st.success("Cleansing Done!")

            st.markdown("---")
            st.subheader("Preview Output Data")
            st.dataframe(df_f.head(100), use_container_width=True)

            st.download_button(
                "Download Output (Excel)",
                data=out_bytes,
                file_name=out_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )