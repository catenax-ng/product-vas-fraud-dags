from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def base64_encode(s):
    from base64 import b64encode

    bs = s.encode('UTF-8')
    b64 = b64encode(bs)
    return b64.decode('UTF-8')


def rmq_publish(rmq_username, rmq_password, rmq_url, routing_key, data):
    from json import dumps
    from requests import post

    payload_str = dumps(data)
    payload_b64_str = base64_encode(payload_str)

    rmq_data = {
        'properties': {},
        'routing_key': routing_key,
        'payload': payload_b64_str,
        'payload_encoding': 'base64'
    }

    credentials = str.join(':', [rmq_username, rmq_password])
    credentials_encoded = base64_encode(credentials)

    response = post(
        url=rmq_url,
        json=rmq_data,
        headers={'Authorization': f'Basic {credentials_encoded}'}
    )

    if response.status_code != 200:
        raise ValueError(f'Status code {response.status_code}')


def rmq_task():
    from airflow.models import Variable

    rmq_username = Variable.get('rmq_username')
    rmq_password = Variable.get('rmq_password')
    rmq_queue_name = Variable.get('rmq_fraud_sync_queue_name')
    rmq_url = Variable.get('rmq_url')

    fraud_sync_api_key = Variable.get('cdq_api_key')
    rmq_publish(rmq_username, rmq_password, rmq_url, rmq_queue_name, fraud_sync_api_key)


with DAG(
    'fraud_sync_dag',
    description='Fraud Sync DAG pinging RabbitMQ',
    schedule_interval=timedelta(minutes=5.0),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['rabbitmq', 'fraud-sync'],
) as dag:
    t1 = PythonOperator(
        task_id='rmq_task',
        python_callable=rmq_task,
    )
