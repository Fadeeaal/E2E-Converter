import streamlit as st

st.set_page_config(
    page_title="E2E Supply Chain Converter",
    page_icon="📊",
    layout="wide"
)

# =========================
# HEADER
# =========================
st.title("📊 E2E Supply Chain Converter")
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
    Welcome to the **E2E Supply Chain Converter** 👋  

    This converter is designed to streamline Supply Chain operations through 
    **automated data cleansing**, **database conversion**, and **planning consolidation**.

    👉 Please select a feature from the **sidebar on the left** to get started.
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
        "Extract and clean Delivery Plan data from multiple sheets "
        "to support supply and planning analysis."
    )

with c2:
    st.subheader("📤 Good Issue")
    st.write(
        "Aggregate and clean Good Issue (GI) data, ensuring it is "
        "ready for reporting and Power BI visualization."
    )

with c3:
    st.subheader("📈 ROFO")
    st.write(
        "Multi-file ROFO compiler (Local & Export) featuring M0–M3 logic, "
        "Primary Sales consolidation, and automated Excel exports."
    )

st.markdown("")

# =========================
# FEATURE CARDS - ROW 2
# =========================
c4, c5 = st.columns(2)

with c4:
    st.subheader("🧹 Opening Stock (ZCORIN)")
    st.write(
        "A unified solution for ZCORIN data: Cleaner (data transformation, "
        "shelf life, and release time) and Converter (Master Data management "
        "integrated with NeonDB)."
    )

with c5:
    st.subheader("📊 DPS MPS")
    st.write(
        "DPS data converter for Local and Export modes, including automated "
        "Primary Sales merging for centralized demand planning consolidation."
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