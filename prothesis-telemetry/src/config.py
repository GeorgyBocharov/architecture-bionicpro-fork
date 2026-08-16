import os
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class Config:
    kafka_bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic: str = os.getenv('KAFKA_TOPIC', 'prothesis-telemetry')
    kafka_client_id: str = os.getenv('KAFKA_CLIENT_ID', 'prothesis-simulator')
    
    send_interval: int = int(os.getenv('SEND_INTERVAL', 3)) 
    
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')

config = Config()