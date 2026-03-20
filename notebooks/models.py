import json
from dataclasses import dataclass, asdict

@dataclass
class Crime:
    '''Define a dataclass for the message, giving a clear schema for each crime record'''
    event_time: str
    crime_type: str
    community_area: int
    latitude: float
    longitude: float


def crime_from_row(row) -> Crime:
    """Convert a DataFrame row to a Crime dataclass instance."""
    return Crime(
        event_time=row['event_time'],
        crime_type=row['crime_type'],
        community_area=int(row['community_area']),
        latitude=float(row['latitude']),
        longitude=float(row['longitude'])
    )


def crime_serializer(crime: Crime) -> bytes:
    '''Custom Serialization for Kafka producer'''
    crime_dict = asdict(crime)
    json_str = json.dumps(crime_dict)
    return json_str.encode('utf-8')


def crime_deserializer(data: bytes) -> Crime:
    '''Deserialization for Kafka consumer'''
    json_str = data.decode('utf-8')
    crime_dict = json.loads(json_str)
    return Crime(**crime_dict)
