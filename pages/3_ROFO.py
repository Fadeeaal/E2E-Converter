import io
import pandas as pd
import streamlit as st

st.set_page_config(page_title="ROFO Compiler", layout="wide")
st.title("ROFO Compiler")

uploaded_files = st.file_uploader(
    "Upload file SOP (.xlsx) - bisa lebih dari 1 file",
    type=["xlsx"],
    accept_multiple_files=True
)

# ====== INPUT TAHUN & BULAN ======
c1, c2 = st.columns(2)
with c1:
    base_year = st.number_input("Tahun M0", min_value=2000, max_value=2100, value=2025, step=1)
with c2:
    base_month = st.number_input("Bulan M0 (1-12)", min_value=1, max_value=12, value=10, step=1)

# ====== LOCKED FILTER ======
FILTER_DISTRIBUTOR = "NATIONAL"
FILTER_UOM = "CARTON"
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
    # Kita merge hanya dengan SKU Code (1 kolom)
    candidates = ["SKU CODE"]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "SKU" in str(c).upper():
            return c
    raise KeyError("Kolom SKU Code tidak ditemukan di sheet.")

def read_filtered(excel_file, sheet_name: str, year_filter: int) -> pd.DataFrame:
    """Baca 1 file+sheet, filter by distributor/uom/year saja (tanpa CYCLE)."""
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=1, engine="openpyxl")
    df = df.loc[:, ~df.columns.isna()]
    df = df.drop(columns=[c for c in df.columns if str(c).startswith("Unnamed")], errors="ignore")

    df = df[
        (df["DISTRIBUTOR"].astype(str).str.strip().str.upper() == FILTER_DISTRIBUTOR) &
        (df["UoM"].astype(str).str.strip().str.upper() == FILTER_UOM) &
        (df["YEAR"].fillna(-1).astype(int) == year_filter)
    ].copy()

    return df

def process_sheet_multi(files, sheet_name: str, base_year: int, base_month: int) -> pd.DataFrame:
    # 1) Cari base_df dari YEAR=base_year (untuk ambil master SKU + metadata)
    base_df = None
    for f in files:
        try:
            tmp = read_filtered(f, sheet_name, base_year)
            if not tmp.empty:
                base_df = tmp
                break
        except Exception:
            continue

    if base_df is None or base_df.empty:
        return pd.DataFrame()

    # drop kolom yang tidak mau ikut
    drop_cols = [
        "Magnitude PH L1 Code", "Magnitude PH L1 Description",
        "Magnitude PH L2 Code", "Magnitude PH L2 Description",
        "Magnitude PH L4 Code", "Magnitude PH L4 Description",
        "RF Product Group L1", "FY", "Cek ", "Cek .1", "TON2CTN",
    ]
    base_df = base_df.drop(columns=drop_cols, errors="ignore")

    sku_col = find_sku_col(base_df)

    # meta cols = semua selain Jan-Dec
    meta_cols = [c for c in base_df.columns if c not in month_names]
    out = base_df[meta_cols].copy()

    # init M0..M3
    for i in range(4):
        out[f"M{i}"] = pd.NA

    # 2) Isi M0..M3: tiap M punya target YEAR + target month header
    for i in range(4):
        yi, mi = add_months(base_year, base_month, i)
        month_col = month_names[mi - 1]

        found = None
        for f in files:
            try:
                tmp = read_filtered(f, sheet_name, yi)
                if tmp.empty or month_col not in tmp.columns:
                    continue

                sku_tmp = find_sku_col(tmp)
                tmp2 = tmp[[sku_tmp, month_col]].copy()
                tmp2 = tmp2.rename(columns={sku_tmp: sku_col, month_col: f"M{i}"})
                found = tmp2
                break
            except Exception:
                continue

        if found is not None:
            out = out.merge(found, on=sku_col, how="left", suffixes=("", "_new"))
            if f"M{i}_new" in out.columns:
                out[f"M{i}"] = out[f"M{i}"].combine_first(out[f"M{i}_new"])
                out = out.drop(columns=[f"M{i}_new"])

        # round
        out[f"M{i}"] = pd.to_numeric(out[f"M{i}"], errors="coerce").round(0).astype("Int64")

    return out

if uploaded_files:
    if st.button("Start process ROFO"):
        with st.spinner("Processing..."):
            ps = process_sheet_multi(uploaded_files, "PS_DRY", int(base_year), int(base_month))
            ss = process_sheet_multi(uploaded_files, "SS_DRY", int(base_year), int(base_month))

        if ps.empty and ss.empty:
            st.error("Tidak ada data yang cocok ditemukan. Cek file dan input Tahun/Bulan M0.")
            st.stop()

        st.success("Selesai!")
        st.write(f"Rows PS: {len(ps)} | Rows SS: {len(ss)}")
        st.dataframe(ps, use_container_width=True)
        st.dataframe(ss, use_container_width=True)

        output = io.BytesIO()
        file_name = f"ROFO_{int(base_year)}-{int(base_month):02d}.xlsx"
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            if not ps.empty:
                ps.to_excel(writer, sheet_name="PS", index=False)
            if not ss.empty:
                ss.to_excel(writer, sheet_name="SS", index=False)
        output.seek(0)

        st.download_button(
            "Download ROFO (Excel)",
            data=output,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.caption("Upload minimum 1 SOP file to start the process.")
