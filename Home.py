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
    "Centralized tools for data cleansing, consolidation, and analytics "
    "across end-to-end Supply Chain processes."
)

st.markdown("---")

# =========================
# INTRO
# =========================
st.markdown(
    """
    Welcome to the **E2E Dashboard** 👋  

    This dashboard serves as a **central hub** for operational and analytical tools,  
    covering **planning**, **execution**, and **data cleansing** activities.

    👉 Please select a feature from the **left sidebar (Pages)** to get started.
    """
)

st.markdown("")

# =========================
# FEATURE CARDS
# =========================
c1, c2, c3 = st.columns(3)

with c1:
    st.subheader("📦 Delivery Plan")
    st.write(
        "Extract and clean Delivery Plan data from multiple sheets "
        "for supply and planning analysis."
    )

with c2:
    st.subheader("📤 Good Issue")
    st.write(
        "Clean and aggregate Good Issue data, "
        "ready for reporting and Power BI visualization."
    )

with c3:
    st.subheader("📈 ROFO")
    st.write(
        "Multi-file ROFO compiler (PS & SS) with M0–M3 logic "
        "and database integration."
    )

st.markdown("")

c4, c5 = st.columns(2)

with c4:
    st.subheader("🧹 ZCORIN Cleaner")
    st.write(
        "Filter and transform ZCORIN data, including "
        "release time calculation, shelf life, and master data enrichment."
    )

with c5:
    st.subheader("🔁 ZCORIN Converter")
    st.write(
        "Master data management tool backed by NeonDB "
        "for ZCORIN conversion and enrichment."
    )

st.markdown("---")

# =========================
# FOOTER
# =========================
st.caption(
    "⚙️ Built with Streamlit | "
    "📊 Integrated with NeonDB | "
    "🔒 Internal Supply Chain Tools"
)