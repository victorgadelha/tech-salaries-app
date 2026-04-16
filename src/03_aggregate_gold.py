import duckdb

def aggregate_gold():
    con = duckdb.connect()

    con.execute("""
        COPY (
            SELECT 
                work_year,
                experience_level,
                AVG(salary_in_usd) AS avg_salary
            FROM 'data/silver/salaries_cleaned.parquet'
            GROUP BY work_year, experience_level
        ) 
        TO 'data/gold/salary_by_year_exp.parquet' 
        (FORMAT 'PARQUET')
    """)

    con.execute("""
        COPY (
            SELECT 
                job_title,
                AVG(salary_in_usd) AS avg_salary
            FROM 'data/silver/salaries_cleaned.parquet'
            GROUP BY job_title
            ORDER BY avg_salary DESC
            LIMIT 10
        ) 
        TO 'data/gold/top_10_jobs.parquet' 
        (FORMAT 'PARQUET')
    """)

    con.close()

if __name__ == "__main__":
    aggregate_gold()