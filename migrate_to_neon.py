import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

LOCAL_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
NEON_URL = os.getenv("NEON_DATABASE_URL")

local_engine = create_engine(LOCAL_URL)
neon_engine = create_engine(NEON_URL)

print("Connected to both databases.\n")

def migrate_table(table, conflict_col=None):
    print(f"Migrating {table}...")
    with local_engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
    print(f"  Read {len(df)} rows locally.")

    if df.empty:
        print(f"  Skipping — empty.\n")
        return

    if 'id' in df.columns:
        df = df.drop(columns=['id'])

    # For tables with unique constraints, insert row by row with ON CONFLICT
    if conflict_col:
        cols = ', '.join(df.columns)
        placeholders = ', '.join([f':{c}' for c in df.columns])
        sql = f"""
            INSERT INTO {table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_col}) DO NOTHING
        """
        chunk_size = 1000
        total = 0
        with neon_engine.connect() as conn:
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                rows = chunk.to_dict(orient='records')
                conn.execute(text(sql), rows)
                conn.commit()
                total += len(chunk)
                print(f"  {total}/{len(df)} rows...")
    else:
        # Fast bulk insert for tables without conflicts
        chunk_size = 5000
        total = 0
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            chunk.to_sql(table, neon_engine, if_exists='append', index=False, method='multi')
            total += len(chunk)
            print(f"  {total}/{len(df)} rows...")

    print(f"  ✅ {table} done.\n")

# Clear existing partial data
with neon_engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE technical_indicators, etf_prices, etf_metadata, pipeline_runs RESTART IDENTITY CASCADE"))
    conn.commit()
    print("Cleared existing Neon data.\n")

migrate_table('pipeline_runs')
migrate_table('etf_metadata', conflict_col='ticker')
migrate_table('etf_prices', conflict_col='ticker, date')
migrate_table('technical_indicators', conflict_col='ticker, date')

print("Migration complete!")