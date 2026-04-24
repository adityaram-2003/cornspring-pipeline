import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# Use Neon in production, local as fallback
DATABASE_URL = os.getenv("NEON_DATABASE_URL") or (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DATABASE_URL, pool_size=5, max_overflow=10)

def get_engine():
    return engine

def test_connection():
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        print("Connected:", result.fetchone()[0])

if __name__ == "__main__":
    test_connection()