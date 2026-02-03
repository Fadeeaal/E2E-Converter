import streamlit as st

st.set_page_config(
    page_title="E2E Supply Chain Dashboard",
    page_icon="📊",
    layout="wide"
)

# =========================
# HEADER
# =========================
st.title("📊 E2E Supply Chain Dashboard")
st.caption(
    "Centralized hub for data cleansing, consolidation, and operational tools "
    "across end-to-end Supply Chain processes."
)

st.markdown("---")

# =========================
# INTRO
# =========================
st.markdown(
    """
    Welcome to the **E2E Dashboard** 👋  

    Dashboard ini dirancang untuk mempermudah operasional tim Supply Chain dalam melakukan 
    **data cleansing**, **konversi database**, hingga **planning consolidation**.

    👉 Silakan pilih fitur melalui **sidebar di sebelah kiri** untuk memulai.
    """
)

st.markdown("")

# =========================
# FEATURE CARDS - ROW 1
# =========================
c1, c2, c3 = st.columns(3)

with c1:
    st.subheader("📦 Delivery Plan")
    st.write(
        "Ekstrak dan pembersihan data Delivery Plan dari berbagai sheet "
        "untuk kebutuhan analisis supply dan planning."
    )

with c2:
    st.subheader("📤 Good Issue")
    st.write(
        "Agregasi dan pembersihan data Good Issue (GI) agar siap "
        "digunakan untuk pelaporan dan visualisasi Power BI."
    )

with c3:
    st.subheader("📈 ROFO")
    st.write(
        "Multi-file ROFO compiler (Local & Export) dengan logika M0–M3, "
        "fitur penggabungan Primary Sales, dan download format Excel."
    )

st.markdown("")

# =========================
# FEATURE CARDS - ROW 2
# =========================
c4, c5 = st.columns(2)

with c4:
    st.subheader("🧹 ZCORIN Tool")
    st.write(
        "Solusi terpadu untuk data ZCORIN: Cleaner (transformasi data, "
        "shelf life, release time) dan Converter (Master Data management "
        "yang terintegrasi dengan NeonDB)."
    )

with c5:
    st.subheader("📊 DPS Tool")
    st.write(
        "Converter data DPS (Local & Export) dengan fitur penggabungan otomatis "
        "data Primary Sales untuk konsolidasi demand planning."
    )

st.markdown("---")

# =========================
# FOOTER
# =========================
st.caption(
    "⚙️ Built with Streamlit | "
    "📊 Integrated with NeonDB | "
    "🔒 Internal Supply Chain Tools - Danone Indonesia"
)