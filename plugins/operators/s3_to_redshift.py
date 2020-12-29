from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    __COPY_SQL__ = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS {}
    """

    __DELETE_SQL__ = """
        DELETE FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_key='',
                 format='',
                 region='',
                 *args, **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_key = s3_key
        self.format = format
        self.region = region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info('Connect to Redshift.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Delete all the existing records.')
        redshift.run(self.__DELETE_SQL__.format(self.table))

        formatted_sql = self.__COPY_SQL__.format(
            self.table,
            self.s3_key,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.format
        )

        self.log.info('Run COPY command.')
        redshift.run(formatted_sql)
