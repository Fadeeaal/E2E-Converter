import streamlit as st
from sqlalchemy import create_engine

def get_engine():
    p = st.secrets["postgres"]
    url = f"postgresql+psycopg2://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['database']}"
    return create_engine(url, pool_pre_ping=True)
