import duckdb

def transform_silver():
    con = duckdb.connect()

    con.execute("""
        COPY (
            SELECT 
                * REPLACE (
                    LOWER(job_title) AS job_title,
                    TRY_CAST(REPLACE(work_year, 'e', '') AS INTEGER) AS work_year
                )
            FROM 'data/bronze/salaries_raw.parquet'
            WHERE salary > 0 
              AND salary IS NOT NULL 
              AND TRY_CAST(REPLACE(work_year, 'e', '') AS INTEGER) IS NOT NULL
        ) 
        TO 'data/silver/salaries_cleaned.parquet' 
        (FORMAT 'PARQUET')
    """)

    con.execute("""
        COPY (
            SELECT * FROM 'data/bronze/salaries_raw.parquet'
            WHERE salary <= 0 
               OR salary IS NULL 
               OR TRY_CAST(REPLACE(work_year, 'e', '') AS INTEGER) IS NULL
        ) 
        TO 'data/quarantine/salaries_anomalies.parquet' 
        (FORMAT 'PARQUET')
    """)

    con.close()

if __name__ == "__main__":
    transform_silver()