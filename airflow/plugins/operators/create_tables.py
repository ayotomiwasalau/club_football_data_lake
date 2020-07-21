from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class Create_Table_in_Redshift(BaseOperator):
    
    ui_color = '#7A3D69'
    
    def __init__(self, 
                 redshift_conn="", 
                 table="",
                 sql="",
                 #provide_context=True
                 *args, **kwargs):
        
        
        super(Create_Table_in_Redshift, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn = redshift_conn
        self.sql = sql
        
        
        
    
    def execute(self, context):
                
        self.log.info('......connecting to redshift........')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn)
        
        self.log.info('......check and delete if table exist........') 
        redshift.run("DROP TABLE IF EXISTS {} CASCADE".format(self.table))
        
        redshift.run(self.sql)
        #formatted_create_tbl_query = Create_Table_in_Redshift.XXXcreate_table_sql.format()
        
        
    
   