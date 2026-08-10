from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import clickhouse_connect
import jwt
import logging
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from jwt import PyJWKClient


load_dotenv()

# ==================== КОНФИГУРАЦИЯ ====================
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
KEYCLOAK_ALGORYTHM = os.getenv("KEYCLOAK_REALM", "RS256")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "reports-api")
KEYCLOAK_PUBLIC_KEY = os.getenv("KEYCLOAK_PUBLIC_KEY", "").replace('\\n', '\n')

# URL для получения JWKS (публичных ключей)
JWKS_URL = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"

# Инициализация клиента для получения ключей
jwks_client = PyJWKClient(JWKS_URL)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "reports_db")

# ==================== МОДЕЛИ ====================
class ArmTelemetryResponse(BaseModel):
    """Модель для телеметрии протеза руки"""
    prothesis_id: str
    avg_battery_level: float
    avg_temperature: float
    avg_grip_strength: float
    avg_wrist_rotation: float

class LegTelemetryResponse(BaseModel):
    """Модель для телеметрии протеза ноги"""
    prothesis_id: str
    avg_battery_level: float
    avg_temperature: float
    avg_speed: float
    avg_pressure_heel: float
    avg_pressure_toe: float

class ReportResponse(BaseModel):
    period_start: str
    period_end: str
    arms: List[ArmTelemetryResponse]
    legs: List[LegTelemetryResponse]

class ErrorResponse(BaseModel):
    detail: str


security = HTTPBearer()
logger = logging.getLogger(__name__)

def get_user_id_from_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    Извлекает user_id из JWT токена Keycloak с логированием
    """
    request_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
    token_preview = credentials.credentials[:20] + "..." if credentials.credentials else "None"
    
    logger.info(f"[{request_id}] Starting token validation. Token preview: {token_preview}")
    
    try:
        token = credentials.credentials
        
        if not token:
            logger.warning(f"[{request_id}] Empty token received")
            raise HTTPException(status_code=401, detail="Empty token")
        
        logger.debug(f"[{request_id}] Attempting to get signing key for token")

        
        # Декодируем и верифицируем токен
        try:
            payload = jwt.decode(
                token,
                KEYCLOAK_PUBLIC_KEY,
                algorithms=["RS256"],
                audience=KEYCLOAK_CLIENT_ID,
                options={"verify_aud": False}
            )
            logger.debug(f"[{request_id}] Token successfully decoded")
        except jwt.ExpiredSignatureError as e:
            logger.warning(f"[{request_id}] Token has expired")
            raise HTTPException(status_code=401, detail="Token has expired")
        except jwt.InvalidAudienceError as e:
            logger.warning(f"[{request_id}] Invalid audience. Expected: {KEYCLOAK_CLIENT_ID}")
            raise HTTPException(status_code=401, detail=f"Invalid audience. Expected: {KEYCLOAK_CLIENT_ID}")
        except jwt.InvalidIssuerError as e:
            logger.warning(f"[{request_id}] Invalid issuer")
            raise HTTPException(status_code=401, detail="Invalid issuer")
        except jwt.InvalidTokenError as e:
            logger.warning(f"[{request_id}] Invalid token: {str(e)}")
            raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")
        except Exception as e:
            logger.error(f"[{request_id}] Unexpected error during token decode: {str(e)}", exc_info=True)
            raise HTTPException(status_code=401, detail=f"Token decode error: {str(e)}")


        azp = payload.get("azp")
        logger.info(f"[{request_id}] azp is {azp}")
        
        # Извлекаем user_id только из поля sub
        user_id = payload.get("sub")
        
        if not user_id:
            logger.error(f"[{request_id}] User ID (sub) not found in token")
            raise HTTPException(status_code=401, detail="User ID (sub) not found in token")
        
        # Логируем успешную аутентификацию
        logger.info(f"[{request_id}] Successfully authenticated user: {user_id}")
        
        return str(user_id)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Unexpected error in token validation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=401, detail=f"Authentication error: {str(e)}")

def get_clickhouse_client():
    """Подключение к ClickHouse"""
    try:
        return clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to ClickHouse: {str(e)}")

# ==================== ПРИЛОЖЕНИЕ ====================
app = FastAPI(
    title="Prosthesis Telemetry Report API",
    version="1.0.0",
    description="API для получения средних показателей телеметрии протезов"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],  # OPTIONS обязателен
    allow_headers=["Authorization", "Content-Type"],  
    expose_headers=["*"],
    max_age=3600, 
)

@app.get(
    "/v1/report",
    response_model=ReportResponse,
    summary="Получить отчет по телеметрии",
    description="Возвращает средние показатели телеметрии для протезов рук и ног за указанный период"
)
async def get_report(
    period_start: str = Query(..., description="Начало периода (YYYY-MM-DD)", example="2026-01-01"),
    period_end: str = Query(..., description="Конец периода (YYYY-MM-DD)", example="2026-01-31"),
    user_id: str = Depends(get_user_id_from_token)
):
    return getReportByUserAndPeriod(period_start, period_end, user_id)

@app.get(
    "/v1/reportUnsafe",
    response_model=ReportResponse,
    summary="Получить отчет по телеметрии (без аутентификации)",
    description="Возвращает средние показатели телеметрии для протезов рук и ног за указанный период"
)
async def get_report_unsafe(
    period_start: str = Query(..., description="Начало периода (YYYY-MM-DD)", example="2026-01-01"),
    period_end: str = Query(..., description="Конец периода (YYYY-MM-DD)", example="2026-01-31"),
    user_id: str = Query(..., description="id пользователя", example="dbd3874d-23f2-40cc-b6cc-3710fe27b5ba")
):
    return getReportByUserAndPeriod(period_start, period_end, user_id)

def getReportByUserAndPeriod(period_start, period_end, user_id):
    """Получение средних показателей телеметрии за период"""
    try:
        # Валидация дат
        try:
            start_date = datetime.strptime(period_start, "%Y-%m-%d")
            end_date = datetime.strptime(period_end, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Expected YYYY-MM-DD")
        
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="period_start must be before period_end")
        
        start_datetime = start_date.replace(hour=0, minute=0, second=0)
        end_datetime = end_date.replace(hour=23, minute=59, second=59)
        
        # Подключение к ClickHouse
        client = get_clickhouse_client()
        
        # Запрос для протезов рук
        arms_query = f"""
            SELECT 
                prothesis_id,
                avg(battery_level) as avg_battery_level,
                avg(temperature) as avg_temperature,
                avg(grip_strength) as avg_grip_strength,
                avg(wrist_rotation) as avg_wrist_rotation
            FROM {CLICKHOUSE_DATABASE}.prothesis_telemetry_arm
            WHERE user_id = '{user_id}'
                AND timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
            GROUP BY prothesis_id
            ORDER BY prothesis_id
        """
        
        # Запрос для протезов ног
        legs_query = f"""
            SELECT 
                prothesis_id,
                avg(battery_level) as avg_battery_level,
                avg(temperature) as avg_temperature,
                avg(speed) as avg_speed,
                avg(pressure_heel) as avg_pressure_heel,
                avg(pressure_toe) as avg_pressure_toe
            FROM {CLICKHOUSE_DATABASE}.prothesis_telemetry_leg
            WHERE user_id = '{user_id}'
                AND timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
            GROUP BY prothesis_id
            ORDER BY prothesis_id
        """
        
        arms_result = client.query(arms_query)
        legs_result = client.query(legs_query)
        
        # Формирование ответа для рук
        arms_data = []
        for row in arms_result.result_rows:
            arms_data.append(ArmTelemetryResponse(
                prothesis_id=row[0],
                avg_battery_level=float(row[1]),
                avg_temperature=float(row[2]),
                avg_grip_strength=float(row[3]),
                avg_wrist_rotation=float(row[4])
            ))
        
        # Формирование ответа для ног
        legs_data = []
        for row in legs_result.result_rows:
            legs_data.append(LegTelemetryResponse(
                prothesis_id=row[0],
                avg_battery_level=float(row[1]),
                avg_temperature=float(row[2]),
                avg_speed=float(row[3]),
                avg_pressure_heel=float(row[4]),
                avg_pressure_toe=float(row[5])
            ))
        
        return ReportResponse(
            period_start=period_start,
            period_end=period_end,
            arms=arms_data,
            legs=legs_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/health")
async def health_check():
    """Проверка состояния сервиса"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )