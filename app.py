import requests
import streamlit as st

API_BASE_URL = "http://localhost:8000"
AVAILABLE_SOURCES = ["gutenberg"]

st.title("Boogle Metadata Explorer")

source = st.selectbox("Source", AVAILABLE_SOURCES, index=0)
book_id = st.text_input("Book ID", value="1")

if st.button("Extract Metadata"):
    with st.spinner("Extracting..."):
        try:
            response = requests.get(f"{API_BASE_URL}/metadata/{source}/{book_id}", timeout=30)
            response.raise_for_status()
            metadata = response.json()
            st.json(metadata)
        except requests.exceptions.RequestException as e:
            st.error(f"Error: {e}")
