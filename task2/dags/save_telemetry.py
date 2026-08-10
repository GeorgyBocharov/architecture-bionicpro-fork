from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

logger = logging.getLogger(__name__)

def get_clickhouse_client():
    """Получение клиента ClickHouse из подключения"""
    conn = BaseHook.get_connection('clickhouse_default')
    
    client = Client(
        host=conn.host or 'clickhouse',
        port=conn.port or 9000,
        user=conn.login or 'airflow_user',
        password=conn.password or 'airflow_password',
        database=conn.schema or 'default'
    )
    
    logger.info(f"✅ Connected to ClickHouse at {conn.host}:{conn.port}")
    return client

def save_telemetry():
    telemetryByEmail = read_kafka()
    usersByEmail = read_users(telemetryByEmail.keys())
    insert_into_clickhouse(telemetryByEmail, usersByEmail)

def read_kafka(): 
    consumer = KafkaConsumer(
        'prothesis-telemetry',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='airflow-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    logger.info("Starting Kafka consumer")
    messages = defaultdict(list)
    try:
        records = consumer.poll(timeout_ms=1000, max_records=100)
        logger.info(f"Consumed messages from {len(records)} partitions")
        consumer.commit()
    except Exception as e:
         logger.error(f"Kafka read failed: {e}")
    finally:
        consumer.close()
    
    for partition, msgs in records.items():
        logger.info(f"handling {len(msgs)} from partition {partition}")
        for msg in msgs:
            email = msg.value.get('email')
            messages[email].append(msg.value)
    
    logger.info(f"Read messages for emails {messages.keys()}")

    return messages

def read_users(emails):
    pg_hook = PostgresHook(postgres_conn_id='keycloak_db_default')
    result = {}
    try:
        placeholders = ','.join(['%s'] * len(emails))
        
        sql_query = f"""
        SELECT id, email, first_name, last_name 
        FROM user_entity 
        WHERE email IN ({placeholders})
        """
        
        logger.info(f"Executing query for {len(emails)} emails")
        
        rows = pg_hook.get_records(sql=sql_query, parameters = list(emails))
        logger.info(f"selected {len(rows)} rows")
    except Exception as e:
        logger.error(f" Error reading users: {str(e)}")
        logger.error(f"   Traceback: {traceback.format_exc()}")

    for row in rows:
        user_id, email, first_name, last_name = row
        result[email] = {
            'id': user_id,
            'first_name': first_name or '',
            'last_name': last_name or '',
            'full_name': f"{first_name or ''} {last_name or ''}".strip()
        }
        
    return result


def insert_into_clickhouse(telemetryByEmail, userInfoByEmail):
    """
    Вставка телеметрии в ClickHouse (таблицы уже созданы)
    """
    client = get_clickhouse_client()
    
    leg_data = []
    arm_data = []
    
    for email, telemetry_list in telemetryByEmail.items():
        user_info = userInfoByEmail.get(email, {})
        user_id = user_info.get('id', '')
        first_name = user_info.get('first_name', '')
        last_name = user_info.get('last_name', '')
        full_name = user_info.get('full_name', '')
        
        for record in telemetry_list:
            # Парсим timestamp из формата 2026-08-08T10:43:13.863304
            timestamp_str = record.get('timestamp')
            
            timestamp = datetime.strptime(timestamp_str.split('.')[0], '%Y-%m-%dT%H:%M:%S')
            
            base_record = {
                'timestamp': timestamp,
                'prothesis_id': record.get('prothesis_id'),
                'user_id': user_id,
                'user_email': email,
                'user_first_name': first_name,
                'user_last_name': last_name,
                'user_full_name': full_name,
                'battery_level': record.get('battery_level'),
                'temperature': record.get('temperature'),
            }
            
            prothesis_type = record.get('prothesis_type', '').lower()
            
            if prothesis_type == 'leg':
                leg_record = base_record.copy()
                leg_record.update({
                    'speed': record.get('speed'),
                    'pressure_heel': record.get('pressure_heel'),
                    'pressure_toe': record.get('pressure_toe'),
                })
                leg_data.append(leg_record)
                
            elif prothesis_type == 'arm':
                arm_record = base_record.copy()
                arm_record.update({
                    'grip_strength': record.get('grip_strength'),
                    'wrist_rotation': record.get('wrist_rotation'),
                })
                arm_data.append(arm_record)
    
    
    total_leg_inserted = 0
    total_arm_inserted = 0
    
    if leg_data:
        logger.info(f"📝 Inserting {len(leg_data)} leg telemetry records...")
        
        leg_records = [[
            row['timestamp'],
            row['prothesis_id'],
            row['user_id'],
            row['user_email'],
            row['user_first_name'],
            row['user_last_name'],
            row['user_full_name'],
            row['battery_level'],
            row['temperature'],
            row['speed'],
            row['pressure_heel'],
            row['pressure_toe'],
        ] for row in leg_data]
        
        client.execute(
            "INSERT INTO reports_db.prothesis_telemetry_leg "
            "(timestamp, prothesis_id, user_id, user_email, user_first_name, "
            "user_last_name, user_full_name, battery_level, temperature, "
            "speed, pressure_heel, pressure_toe) VALUES",
            leg_records
        )
        total_leg_inserted = len(leg_records)
        logger.info(f"✅ Inserted {total_leg_inserted} leg records")
    
    if arm_data:
        logger.info(f"📝 Inserting {len(arm_data)} arm telemetry records...")
        
        arm_records = [[
            row['timestamp'],
            row['prothesis_id'],
            row['user_id'],
            row['user_email'],
            row['user_first_name'],
            row['user_last_name'],
            row['user_full_name'],
            row['battery_level'],
            row['temperature'],
            row['grip_strength'],
            row['wrist_rotation'],
        ] for row in arm_data]
        
        client.execute(
            "INSERT INTO reports_db.prothesis_telemetry_arm "
            "(timestamp, prothesis_id, user_id, user_email, user_first_name, "
            "user_last_name, user_full_name, battery_level, temperature, "
            "grip_strength, wrist_rotation) VALUES",
            arm_records
        )
        total_arm_inserted = len(arm_records)
        logger.info(f"✅ Inserted {total_arm_inserted} arm records")
    

# ============================================
# DAG
# ============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'save_prothesis_telemetry',
    default_args=default_args,
    description='Create test table and insert data using clickhouse_default',
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['clickhouse', 'test'],
)

# ============================================
# ЗАДАЧИ
# ============================================


save_telemetry_task = PythonOperator(
    task_id='save_telemetry',
    python_callable=save_telemetry,
    dag=dag,
)


end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# ============================================
# ПОРЯДОК ВЫПОЛНЕНИЯ
# ============================================
start_task >> save_telemetry_task >> end_task