# db_conn.py - REPLACE the entire file with this:
import psycopg2
import time

def get_db_connection():
    """
    Establishes and returns a PostgreSQL database connection with timeout settings.
    """
    DB_CONFIG = {
        "host": "localhost",
        "database": "postgres",
        "user": "postgres",
        "password": "123456",
        "connect_timeout": 30,
        "options": "-c statement_timeout=300000 -c lock_timeout=300000"  # 5 minute timeouts
    }
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Set additional timeout parameters
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 300000;")  # 5 minutes
            cur.execute("SET lock_timeout = 300000;")       # 5 minutes
        
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

# Add chunked insert function
def chunked_insert_dataframe(conn, df, table_name, chunk_size=1000):
    """Insert DataFrame in chunks to avoid timeouts"""
    total_rows = len(df)
    chunks = [df[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]
    
    for i, chunk in enumerate(chunks):
        try:
            # Use COPY for each chunk
            output = io.StringIO()
            chunk.to_csv(output, index=False, header=False, na_rep='NULL')
            output.seek(0)
            
            columns_str = ", ".join(df.columns)
            copy_sql = f"COPY spigen.{table_name} ({columns_str}) FROM STDIN WITH CSV NULL 'NULL'"
            
            with conn.cursor() as cur:
                cur.copy_expert(copy_sql, output)
            
            conn.commit()
            print(f"Successfully inserted chunk {i+1} with {len(chunk)} rows")
            
        except Exception as e:
            conn.rollback()
            print(f"Failed to insert chunk {i+1}: {str(e)}")
            raise
    
    return True

# Test the connection when run directly
if __name__ == "__main__":
    try:
        conn = get_db_connection()
        print("✅ Database connection successful!")
        conn.close()
    except Exception as e:
        print(f"❌ Connection failed: {e}")