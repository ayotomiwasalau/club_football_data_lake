from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class Load_s3_to_redshift(BaseOperator):
    
    template_fields=("s3_key",)
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    IGNOREHEADER {}
    DELIMITER '{}'
    ;
    
    """
#     COPY_PARQUET_TABLE = """
#         COPY {schema}.{table} FROM '{s3_uri}'
#         IAM_ROLE '{aws_iam_role}'
#         FORMAT AS PARQUET;
#     ACCESS_KEY_ID '{}'
#     SECRET_ACCESS_KEY '{}'  
#    IAM_ROLE '{}'
#     """    
    def __init__(self,
                redshift_conn="",
                aws_credential_id="",
                table="",
                s3_bucket="",
                s3_key="",
                delimiter=",",
                #aws_iam_role_id="arn:aws:iam::189128986856:role/aws-service-role/redshift.amazonaws.com/AWSServiceRoleForRedshift",
                ignore_headers=1,
                *args, **kwargs):
        
        super(Load_s3_to_redshift, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credential_id = aws_credential_id
        #self.aws_iam_role_id = aws_iam_role_id
        
    
    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info('......connecting to redshift........')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('......copying data from s3 table to redshift........')
        s3_key_output=self.s3_key.format(**context)
        
        ###########*****check
        s3_path="s3://{}/{}".format(self.s3_bucket, s3_key_output)
        
        formatted_sql=Load_s3_to_redshift.copy_sql.format(            
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            #self.aws_iam_role_id
            self.ignore_headers,
            self.delimiter,         
        )
        
        redshift.run(formatted_sql)
        
        
        
    
    