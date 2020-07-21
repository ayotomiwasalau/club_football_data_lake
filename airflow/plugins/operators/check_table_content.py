from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class Check_table_quality(BaseOperator):
    
    ui_color = '#358140'
    
    @apply_defaults 
    def __init__(self,
                redshift_conn_id="",
                tables=[],
                *args, **kwargs):
        
        
        super(Check_table_quality, self).__init__(*args, **kwargs)
        
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
    
    def execute(self, context):
                
        self.log.info('......connecting to redshift........')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        for table in self.tables:
            self.log.info(f'Starting data quality check for {table} in Redhsift')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            # Test 1
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                
            # Test 2
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
                
#             spark.sql("""
#                 SELECT COUNT (DISTINCT cicid)
#                 FROM immig_table
#                 """).show()
        
        
    
    