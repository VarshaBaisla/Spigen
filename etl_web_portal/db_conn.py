# db_conn.py
import psycopg2

def get_db_connection():
    """
    Establishes and returns a PostgreSQL database connection.
    """
    DB_CONFIG = {
        "host": "localhost",
        "database": "postgres",  # Your database name
        "user": "postgres",
        "password": "123456"   # Your password
    }
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

# Test the connection when run directly
if __name__ == "__main__":
    try:
        conn = get_db_connection()
        print("✅ Database connection successful!")
        conn.close()
    except Exception as e:
        print(f"❌ Connection failed: {e}")