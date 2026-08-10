import logging
import signal
import sys
import time
import random

from .config import config
from .telemetry_generator import ProthesisTelemetry, ProthesisRegistry, ProthesisType
from .kafka_producer import KafkaTelemetryProducer

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProthesisTelemetryService:
    """Сервис для отправки телеметрии в Kafka"""
    
    def __init__(self):
        self.running = False
        self.producer = None
        self.registry = ProthesisRegistry()
        self.packet_count = 0
        
        # Инициализация Kafka продюсера
        self.producer = KafkaTelemetryProducer(
            bootstrap_servers=config.kafka_bootstrap_servers,
            client_id=config.kafka_client_id
        )
        
        # Обработка сигналов
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def _generate_telemetry(self) -> dict:
        """Генерация одного пакета телеметрии"""
        prothesis = self.registry.get_prothesis()
        
        telemetry = ProthesisTelemetry.generate_telemetry(
            prothesis_id=prothesis['prothesis_id'],
            email=prothesis['email'],
            prothesis_type=prothesis['prothesis_type']
        )
        
        return telemetry
    
    def _print_telemetry(self, telemetry: dict):
        """Красивый вывод телеметрии в лог"""
        prothesis_type = telemetry.get('prothesis_type', 'unknown')
        
        if prothesis_type == 'leg':
            logger.info(
                f"📤 Packet #{self.packet_count} | "
                f"🦵 LEG | "
                f"{telemetry['prothesis_id']} | "
                f"🔋 {telemetry['battery_level']}% | "
                f"💨 {telemetry['speed']} km/h | "
                f"👣 H:{telemetry['pressure_heel']} T:{telemetry['pressure_toe']} kPa | "
                f"🌡️ {telemetry['temperature']}°C"
            )
        else:  # arm
            logger.info(
                f"📤 Packet #{self.packet_count} | "
                f"🦾 ARM | "
                f"{telemetry['prothesis_id']} | "
                f"🔋 {telemetry['battery_level']}% | "
                f"💪 {telemetry['grip_strength']}N | "
                f"🤏 {telemetry['wrist_rotation']} | "
            )
    
    def start(self):
        """Запуск сервиса"""
        if self.running:
            logger.warning("Service is already running")
            return
        
        # Подключение к Kafka
        if not self.producer.connect():
            logger.error("Failed to connect to Kafka")
            return
        
        self.running = True
        logger.info("=" * 70)
        logger.info("Prothesis Telemetry Service Started (Kafka)")
        logger.info(f"📊 Registered protheses:")
        for p in self.registry.get_all_protheses():
            emoji = "🦵" if p['prothesis_type'] == ProthesisType.LEG else "🦾"
            logger.info(f"   {emoji} {p['email']} -> {p['prothesis_id']} ({p['prothesis_type'].value})")
        logger.info(f"⏱️  Send interval: {config.send_interval}s")
        logger.info(f"📨 Kafka topic: {config.kafka_topic}")
        logger.info(f"🔗 Bootstrap servers: {config.kafka_bootstrap_servers}")
        logger.info("=" * 70)
        
        # Основной цикл
        while self.running:
            try:
                # Генерируем телеметрию
                telemetry = self._generate_telemetry()
                
                # Отправляем в Kafka
                if self.producer.send_telemetry(
                    topic=config.kafka_topic,
                    payload=telemetry,
                    key=telemetry['prothesis_id']
                ):
                    self.packet_count += 1
                    self._print_telemetry(telemetry)
                else:
                    logger.warning("Failed to send telemetry")
                
                # Ждем до следующей отправки
                for _ in range(config.send_interval):
                    if not self.running:
                        break
                    time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(5)
    
    def stop(self):
        """Остановка сервиса"""
        if not self.running:
            return
        
        self.running = False
        if self.producer:
            self.producer.disconnect()
        
        logger.info(f"📊 Total packets sent: {self.packet_count}")
        logger.info("Prothesis telemetry service stopped")

if __name__ == "__main__":
    service = ProthesisTelemetryService()
    try:
        service.start()
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
        service.stop()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Service failed: {e}")
        service.stop()
        sys.exit(1)