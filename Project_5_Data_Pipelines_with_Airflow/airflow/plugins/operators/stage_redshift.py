from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_key="",
                 table="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_key = s3_key
        self.table = table
        self.json_path  = json_path
        self.task_id = kwargs["task_id"]

    def execute(self, context):
        # setup variables
        logging.info(f"Executing task_id: {self.task_id}")
        aws_hook= AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id="redshift")
        s3_bucket = Variable.get('s3_bucket')
        self.s3_key = self.s3_key.format(**context)

        s3_path = f"s3://{s3_bucket}/{self.s3_key}"
        
        redshift.run(f"TRUNCATE {self.table}")
        
        # COPY 
        logging.info("COPY table...")  
        query = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}'" \
                f" SECRET_ACCESS_KEY '{credentials.secret_key}' REGION 'us-west-2'"      
        
        if self.json_path != "auto":
            query = query + f" JSON '{self.json_path}' COMPUPDATE OFF"
        else:
            query = query + " IGNOREHEADER 1 DELIMITER ','"
            
            
        logging.info(f"Running against redshift: \n {query}")
        redshift.run(query)
        #https://knowledge.udacity.com/questions/253565
 
        logging.info(f"Done with task_id: {self.task_id}")



