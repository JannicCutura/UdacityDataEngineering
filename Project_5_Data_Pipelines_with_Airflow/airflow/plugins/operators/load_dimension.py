from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 query="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.table = table
        self.task_id = kwargs["task_id"]

    def execute(self, context):
        logging.info(f"Starting with task_id: {self.task_id}")  
        
        # establish connection
        redshift = PostgresHook(postgres_conn_id="redshift")
        
        # delete old version
        redshift.run(f"TRUNCATE TABLE {self.table}")
        
        # insert new data
        redshift.run(f"INSERT INTO {self.table} ({self.query})")
        
        logging.info(f"Done with task_id: {self.task_id}")
