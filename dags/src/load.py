import os
import pandas as pd
import psycopg2
import logging
from io import StringIO

PROCESSED_PATH = "/opt/airflow/data/processed/cleaned_retail.csv"

def load_data():
    logging.info("Load stage started")

    if not os.path.exists(PROCESSED_PATH):
        raise FileNotFoundError("Processed file not found")

    df = pd.read_csv(PROCESSED_PATH)

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )

    cur = conn.cursor()

    # Create table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS retail_data (
        invoice TEXT,
        stockcode TEXT,
        description TEXT,
        quantity INT,
        invoicedate TIMESTAMP,
        price FLOAT,
        customer_id FLOAT,
        country TEXT,
        totalamount FLOAT
    )
    """)
    conn.commit()

    # 🔥 Convert dataframe to CSV in memory
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # 🔥 FAST bulk insert
    cur.copy_expert("""
        COPY retail_data FROM STDIN WITH CSV
    """, buffer)

    conn.commit()
    cur.close()
    conn.close()

    logging.info("Data loaded into PostgreSQL successfully (bulk insert)")