import pandas as pd
import psycopg2

tripdata = pd.read_parquet('data_lake/silver/tripdata.parquet')

tripdata = tripdata[[
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'total_amount',
    'pulocationid',
    'dolocationid',
    'payment_type'
]]

tripdata.columns = [
    'pickup_datetime',
    'dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'total_amount',
    'pulocationid',
    'dolocationid',
    'payment_type'
]

csv_path = 'tripdata_temp.csv'
tripdata.to_csv(csv_path, index=False)

conn = psycopg2.connect(
    host='localhost',
    database='taxi_dw',
    user='postgres',
    password='002004'
)

cursor = conn.cursor()

with open(csv_path, 'r') as f:
    cursor.copy_expert("""
        COPY fact_trips(
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            fare_amount,
            total_amount,
            pulocationid,
            dolocationid,
            payment_type
        )
        FROM STDIN WITH CSV HEADER
    """, f)

conn.commit()
cursor.close()
conn.close()

print("fact_trips uploaded!")