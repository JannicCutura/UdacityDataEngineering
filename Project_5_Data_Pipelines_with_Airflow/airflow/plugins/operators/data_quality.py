from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables=tables

    def execute(self, context):
        logging.info(f"Starting with task_id: {self.task_id}")
        
        # establish redshift connection
        redshift = PostgresHook(postgres_conn_id="redshift")         
        
        for table in self.tables: 
            logging.info(f"Checking table {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            # copied from lesson 2
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
           
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
 
        logging.info(f"Done with task_id: {self.task_id}")

       