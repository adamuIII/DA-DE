from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import pandas as pa
import requests
import boto3
from airflow.operators.bash_operator import BashOperator

s3_client = boto3.client('s3')
target_bucket_name = 'my-data-transform-bucket-adamr'

url_city = 'https://data.cdc.gov/resource/w6be-99qd.json'


def extract_data(**kwargs):
    url = kwargs['url']
    response = requests.get(url)
    data = response.json()
    df = pa.DataFrame(data)
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = 'cancer_data_' + date_now_string
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    output_list = [output_file_path, file_str]
    return output_list


def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids="tsk_extract_cancer_data")[0]
    object_key = task_instance.xcom_pull(task_ids="tsk_extract_cancer_data")[1]
    df = pa.read_csv(data)

    df = df.dropna()
    df = df.replace('', pa.NA).dropna()
    df = df.replace('NULL', pa.NA).dropna()
    df = df.drop("demographic", axis=1)

    print('Num of rows:', len(df))
    print('Num of cols:', len(df.columns))

    # Convert DataFrame to CSV format
    csv_data = df.to_csv(index=False)

    # Upload CSV to S3
    object_key = f"{object_key}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG('cancer_stats_dag',
         default_args=default_args,
         # schedule_interval='@weekly',
         catchup=False) as dag:
    extract_cancer_data = PythonOperator(
        task_id='tsk_extract_cancer_data',
        python_callable=extract_data,
        op_kwargs={'url': url_city}
    )

    transform_cancer_data = PythonOperator(
        task_id='tsk_transform_cancer_data',
        python_callable=transform_data
    )

    load_to_s3 = BashOperator(
        task_id='tsk_load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull("tsk_extract_cancer_data")[0]}} s3://my-data-raw-bucket-adamr',
    )
    extract_cancer_data >> transform_cancer_data >> load_to_s3

