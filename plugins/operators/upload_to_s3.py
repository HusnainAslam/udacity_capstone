import os
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UploadToS3Operator(BaseOperator):
    """
    Operator that Upload Files to S3
    """

    ui_color = '#218251'

    @apply_defaults
    def __init__(self,
                 s3_bucket_name,
                 aws_connection_id,
                 prefix,
                 input_dir,
                 *args, **kwargs):

        super(UploadToS3Operator, self).__init__(*args, **kwargs)
        self.s3_bucket_name = s3_bucket_name
        self.prefix = prefix
        self.aws_connection_id = aws_connection_id
        self.input_dir = input_dir

    def execute(self, context):
        """
        Load the data files onto the chosen storage location.
        """
        s3_hook = S3Hook(aws_conn_id=self.aws_connection_id)
        files = [os.path.join(self.input_dir, f) for f in os.listdir(self.input_dir) if os.path.isfile(
            os.path.join(self.input_dir, f))]

        for file in files:
            file_name = file.split('/')[-1]
            self.log.info(f"Start Uploading file {file_name}")
            s3_hook.load_file(
                filename=file_name,
                key=f'{self.prefix}/{file_name}',
                bucket_name=self.s3_bucket_name)
            self.log.info(f"Completed Uploading file {file_name}")

