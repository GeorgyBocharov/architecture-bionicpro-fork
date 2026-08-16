import json
import logging
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

class KafkaTelemetryProducer:
    """Kafka продюсер для отправки телеметрии"""
    
    def __init__(self, 
                 bootstrap_servers: str,
                 client_id: str = "prothesis-producer"):
        
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.producer: Optional[KafkaProducer] = None
        self.connected = False
        
    def connect(self) -> bool:
        """Подключение к Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None,
                acks='all',  # Подтверждение от всех реплик
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip'
            )
            
            # Проверяем подключение
            self.producer.metrics()
            self.connected = True
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.connected = False
            return False
    
    def send_telemetry(self, 
                       topic: str, 
                       payload: dict, 
                       key: Optional[str] = None) -> bool:
        """Отправка телеметрии в Kafka"""
        if not self.connected or not self.producer:
            logger.warning("Not connected to Kafka")
            return False
        
        try:
            # Отправка с ключом для партиционирования (prothesis_id)
            future = self.producer.send(
                topic=topic,
                value=payload,
                key=key or payload.get('prothesis_id')
            )
            
            # Синхронное ожидание результата
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Message sent to topic {record_metadata.topic} "
                        f"partition {record_metadata.partition} "
                        f"offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    def flush(self):
        """Принудительная отправка всех буферизированных сообщений"""
        if self.producer:
            self.producer.flush()
    
    def disconnect(self):
        """Закрытие соединения"""
        if self.producer:
            self.flush()
            self.producer.close()
            self.connected = False
            logger.info("Disconnected from Kafka")