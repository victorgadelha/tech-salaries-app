import duckdb

def ingest_bronze():
    con = duckdb.connect()
    
    con.execute("""
        COPY (
            SELECT 
                *,
                now() AS _load_timestamp,
                'ds_salaries.csv' AS _source_file
            FROM read_csv_auto('data/bronze/ds_salaries.csv')
        ) 
        TO 'data/bronze/salaries_raw.parquet' 
        (FORMAT 'PARQUET')
    """)
    
    con.close()
    print("Funcionou!")

if __name__ == "__main__":
    ingest_bronze()