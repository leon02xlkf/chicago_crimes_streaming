import time
import pandas as pd
from models import crime_serializer, crime_from_row
from kafka import KafkaProducer

CSV_PATH = "data/crimes_2025.csv"
SERVER = "localhost:9092"
TOPIC = "chi_crimes"


columns = ['Date', 'Primary Type', 'Community Area', 'Latitude', 'Longitude']
df = pd.read_csv(CSV_PATH, usecols=columns).head(1000)
df = df.rename(columns={
    'Date': 'event_time',
    'Primary Type': 'crime_type',
    'Community Area': 'community_area',
    'Latitude': 'latitude',
    'Longitude': 'longitude'
})
df['event_time'] = pd.to_datetime(df['event_time'])
df = df.dropna(subset=['community_area', 'latitude', 'longitude']) # Drop rows with missing values in critical fields

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=crime_serializer
)

t0 = time.time()
for _, row in df.iterrows():
    crime = crime_from_row(row)
    producer.send(TOPIC, value=crime)
    print(f"Sent: {crime}")
    time.sleep(0.01)

producer.flush()
producer.close()

t1 = time.time()
print(f'Sent {len(df)} records, took {(t1 - t0):.2f} seconds')