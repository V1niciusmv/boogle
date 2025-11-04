import streamlit as st
import requests

API_BASE_URL = "http://localhost:8000"

st.title("Gutenberg Metadata Extractor")

book_id = st.number_input("Book ID", min_value=1, value=1, step=1)

if st.button("Extract Metadata"):
    with st.spinner("Extracting..."):
        try:
            response = requests.get(f"{API_BASE_URL}/metadata/{int(book_id)}", timeout=30)
            response.raise_for_status()
            metadata = response.json()
            st.json(metadata)
        except requests.exceptions.RequestException as e:
            st.error(f"Error: {e}")
