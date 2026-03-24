import pandas as pd
import psycopg2

zone = pd.read_parquet('data_lake/silver/zone.parquet')

conn = psycopg2.connect(
    host='localhost',
    database='taxi_dw',
    user='postgres',
    password='002004'
)

cursor = conn.cursor()

for _, row in zone.iterrows():
    cursor.execute("""
        INSERT INTO dim_location (locationid, borough, zone, service_zone)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (locationid) DO NOTHING
    """, tuple(row))

conn.commit()
cursor.close()
conn.close()

print("dim_location uploaded!")