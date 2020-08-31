from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY public.{}
        FROM '{}'        
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
       """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="", 
                 s3_bucket="",
                 s3_key="",                 
                 json_parameter="",                        
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.aws_credentials_id=aws_credentials_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json_parameter=json_parameter             

    def execute(self, context):              
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        self.log.info("get aws credentials")
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Redshift is connected')
        self.log.info("Copying data from S3 to Redshift")        
        s3_path="s3://{}/{}".format(self.s3_bucket,self.s3_key)
        formatted_sql=StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_parameter
        )
        redshift.run(formatted_sql)
        self.log.info('Data is copied from S3 to Redshift')
        
        
        





