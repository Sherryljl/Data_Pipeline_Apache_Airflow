from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 
                 redshift_conn_id="",
                 table="",
                 append_data="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.append_data=append_data
        self.sql_statement=sql_statement

    def execute(self, context):        
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Redshift is connected')
        
        if self.append_data==True:
            insert_sql='INSERT INTO {} {}'.format(self.table,self.sql_statement)
            redshift.run(insert_sql)
        else:
            delete_sql='DELETE FROM {}'.format(self.table)
            redshift.run(delete_sql)
            insert_sql='INSERT INTO {} {}'.format(self.table,self.sql_statement)
            redshift.run(insert_sql)                
        
        self.log.info('{} dimension table is successfully inserted.'.format(self.table))
