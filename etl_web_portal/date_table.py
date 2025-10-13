import psycopg2
from datetime import datetime, timedelta

# --- DB Connection ---
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="123456"
)
cur = conn.cursor()

# --- Get Latest Date ---
cur.execute("SELECT MAX(date_value) FROM spigen.date_table;")
latest_date = cur.fetchone()[0]

if not latest_date:
    print("⚠️ No data found in dim_date. Please insert one starting date first (e.g. '2020-01-01').")
else:
    today = datetime.now().date()
    next_date = latest_date + timedelta(days=1)
    added = 0

    while next_date <= today:
        date_id = int(next_date.strftime("%Y%m%d"))
        cur.execute("""
            INSERT INTO spigen.date_table (date_id, date_value)
            VALUES (%s, %s)
            ON CONFLICT (date_id) DO NOTHING;
        """, (date_id, next_date))
        added += 1
        next_date += timedelta(days=1)

    conn.commit()
    print(f"✅ {added} new date(s) added up to {today}.")

cur.close()
conn.close()
