import streamlit as st

st.set_page_config(page_title="E2E Dashboard", layout="wide")
st.title("E2E Dashboard")
st.caption("Pilih fitur dari sidebar kiri (Pages).")

st.markdown("""
Fitur:
- DeliveryPlan
- GoodIssue
- ROFO
- ZCORIN Cleaner
- ZCORIN Converter
""")
