from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 delete_flag=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.delete_flag = delete_flag

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_flag :
            self.log.info(f"Clearing the Redshift dimension table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Filling up the Redshift dimension table {self.table} with the query.")
        redshift.run(f"INSERT INTO {self.table}" + self.query)
