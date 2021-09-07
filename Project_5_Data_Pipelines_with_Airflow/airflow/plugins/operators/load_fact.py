from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.query = query

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id="redshift")
        logging.info("Connect to Redshift")
        
        query = f"INSERT INTO {self.table} ({self.query})"
        logging.info(f"Running {query}")
        redshift.run(query)
        
        
        
