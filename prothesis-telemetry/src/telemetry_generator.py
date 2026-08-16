import random
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from enum import Enum

class ProthesisType(Enum):
    LEG = "leg"
    ARM = "arm"

class ProthesisTelemetry:
    """Генератор телеметрии для протезов"""
    
    @staticmethod
    def generate_leg_telemetry(prothesis_id: str, email: str) -> Dict[str, Any]:
        """Генерация телеметрии для протеза ноги"""
        return {
            "timestamp": datetime.now().isoformat(),
            "email": email,
            "prothesis_id": prothesis_id,
            "prothesis_type": "leg",
            # Общие параметры
            "battery_level": round(random.uniform(15, 100), 1),
            "temperature": round(random.uniform(25, 45), 1),
            # Специфичные для ноги
            "speed": round(random.uniform(0, 12.0), 2),          # км/ч
            "pressure_heel": round(random.uniform(0, 120), 1),   # кПа
            "pressure_toe": round(random.uniform(0, 100), 1),    # кПа
        }
    
    @staticmethod
    def generate_arm_telemetry(prothesis_id: str, email: str) -> Dict[str, Any]:
        """Генерация телеметрии для протеза руки"""
        return {
            "timestamp": datetime.now().isoformat(),
            "email": email,
            "prothesis_id": prothesis_id,
            "prothesis_type": "arm",
            # Общие параметры
            "battery_level": round(random.uniform(15, 100), 1),
            "temperature": round(random.uniform(25, 45), 1),
            # Специфичные для руки
            "grip_strength": round(random.uniform(0, 50), 1),    # Ньютоны
            "wrist_rotation": round(random.uniform(-180, 180), 1), # градусы
        }
    
    @staticmethod
    def generate_telemetry(prothesis_id: str, 
                          email: str, 
                          prothesis_type: ProthesisType) -> Dict[str, Any]:
        """Генерация телеметрии в зависимости от типа протеза"""
        if prothesis_type == ProthesisType.LEG:
            return ProthesisTelemetry.generate_leg_telemetry(prothesis_id, email)
        else:
            return ProthesisTelemetry.generate_arm_telemetry(prothesis_id, email)

class ProthesisRegistry:
    """Реестр протезов с фиксированными пользователями"""
    
    def __init__(self):
        # Фиксированные пользователи с разными типами протезов
        self.protheses = [
            {
                "email": "prothetic1@example.com",
                "prothesis_id": "PRO-LEG-1",
                "prothesis_type": ProthesisType.LEG,
            },
            {
                "email": "prothetic2@example.com",
                "prothesis_id": "PRO-LEG-2",
                "prothesis_type": ProthesisType.LEG,
            },
            {
                "email": "prothetic3@example.com",
                "prothesis_id": "PRO-ARM-1",
                "prothesis_type": ProthesisType.ARM,
            }
        ]
    
    def get_prothesis(self) -> Dict[str, Any]:
        """Получить случайный протез из реестра"""
        return random.choice(self.protheses)
    
    def get_all_protheses(self) -> List[Dict[str, Any]]:
        """Получить все протезы"""
        return self.protheses
