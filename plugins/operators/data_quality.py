from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here              
                 redshift_conn_id="", 
                 dq_checks="",
                 *args, **kwargs):     

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        
        self.redshift_conn_id= redshift_conn_id
        self.dq_checks=dq_checks
        

    def execute(self, context):
        
        redshift=PostgresHook(self.redshift_conn_id)              
        
        for check in self.dq_checks:
            records=redshift.get_records(check['check_sql'])[0]
            self.log.info('DataQualityOperator checking for NULL ids')            
           
            
            if records[0] != check['expected_result']:
                raise ValueError(f"Data quality check failed. {check['table']} contains null in id column")
            else:
                self.log.info(f"Data quality check passed on {check['table']}")