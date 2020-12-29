from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators.s3_to_redshift import S3ToRedshiftOperator
from plugins.operators.upload_to_s3 import UploadToS3Operator


import configparser

config = configparser.ConfigParser()
config.read('conf.cfg')

S3_BUCKET = config.get('S3', 'BUCKET')
S3_PREFIX = config.get('S3', 'PREFIX')
OUTPUT_DATA_DIR_PATH = config.get('DATA', 'OUTPUT_DATA_DIR_PATH')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 12, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(
    'capstone_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='* 7 * * *',
    max_active_runs=1
)

start_operator = DummyOperator(task_id='Start', dag=dag)
end_operator = DummyOperator(task_id='End', dag=dag)

upload_output_to_s3 = UploadToS3Operator(
    task_id='upload_output_to_s3',
    s3_bucket_name=S3_BUCKET,
    prefix=S3_PREFIX,
    input_dir=OUTPUT_DATA_DIR_PATH,
    aws_connection_id='aws_credentials',
    dag=dag
)

immigration_to_redshift = S3ToRedshiftOperator(
    task_id='immigration_table_load',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='tbl_I94_data',
    s3_key=f's3://{S3_BUCKET}/{S3_PREFIX}/fact_immigration.parquet',
    format="PARQUET",
    region='us-west-2',
    dag=dag
)

city_demographic_to_redshift = S3ToRedshiftOperator(
    task_id='city_demographic_table_load',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='tbl_city_demographic_data',
    s3_key=f's3://{S3_BUCKET}/{S3_PREFIX}/dim_city_demographic',
    format="PARQUET",
    region='us-west-2',
    dag=dag
)

country_to_redshift = S3ToRedshiftOperator(
    task_id='country_table_load',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='tbl_country_data',
    s3_key=f's3://{S3_BUCKET}/{S3_PREFIX}/dim_county.parquet',
    format="PARQUET",
    region='us-west-2',
    dag=dag
)

ports_to_redshift = S3ToRedshiftOperator(
    task_id='ports_table_load',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='tbl_ports_data',
    s3_key=f's3://{S3_BUCKET}/{S3_PREFIX}/dim_port.parquet',
    format="PARQUET",
    region='us-west-2',
    dag=dag
)

state_to_redshift = S3ToRedshiftOperator(
    task_id='state_table_load',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='tbl_state_data',
    s3_key=f's3://{S3_BUCKET}/{S3_PREFIX}/dim_state.parquet',
    format="PARQUET",
    region='us-west-2',
    dag=dag
)

travel_mode_to_redshift = S3ToRedshiftOperator(
    task_id='travel_mode_table_load',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='tbl_travel_mode_data',
    s3_key=f's3://{S3_BUCKET}/{S3_PREFIX}/dim_travel_mode.parquet',
    format="PARQUET",
    region='us-west-2',
    dag=dag
)

visa_to_redshift = S3ToRedshiftOperator(
    task_id='visa_table_load',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='tbl_visa_data',
    s3_key=f's3://{S3_BUCKET}/{S3_PREFIX}/dim_visa.parquet',
    format="PARQUET",
    region='us-west-2',
    dag=dag
)

airport_to_redshift = S3ToRedshiftOperator(
    task_id='airport_table_load',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='tbl_airport_data',
    s3_key=f's3://{S3_BUCKET}/{S3_PREFIX}/dim_airport_data.parquet',
    format="PARQUET",
    region='us-west-2',
    dag=dag
)

start_operator >> \
upload_output_to_s3 >> \
(immigration_to_redshift, city_demographic_to_redshift, country_to_redshift, ports_to_redshift, state_to_redshift,
 travel_mode_to_redshift, visa_to_redshift, airport_to_redshift) >> \
end_operator
