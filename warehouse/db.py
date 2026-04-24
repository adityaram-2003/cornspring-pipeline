import os
from sqlalchemy import create_engine, text

def get_database_url():
    # Try Streamlit secrets first
    try:
        import streamlit as st
        url = st.secrets.get("NEON_DATABASE_URL")
        if url:
            return url
    except Exception:
        pass
    
    # Try environment variable
    url = os.getenv("NEON_DATABASE_URL")
    if url:
        return url
    
    raise ValueError("NEON_DATABASE_URL not found in secrets or environment")

DATABASE_URL = get_database_url()
engine = create_engine(DATABASE_URL, pool_size=5, max_overflow=10)

def get_engine():
    return engine

def test_connection():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        print("Connected:", result.fetchone()[0])

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    DATABASE_URL = os.getenv("NEON_DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    test_connection()