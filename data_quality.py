from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks_list="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks_list = checks_list

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for a_check in self.checks_list :
            records = redshift.get_records(a_check["test_sql"])
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed:\n \
                                  {} {} {} returned no results".format(a_check["test_sql"],a_check["comparison"],a_check["expected_result"]))
                
                
            num_records = records[0][0]
            
            if not eval("{} {} {}".format(num_records,a_check["comparison"],a_check["expected_result"])) :
                raise ValueError("Data quality check failed:\n \
                                  {} {} {}  is not true".format(a_check["test_sql"],a_check["comparison"],a_check["expected_result"]))
                
                
            logging.info("Data quality check passed:\n \
                          {} returned {}".format(a_check["test_sql"],records[0][0]))
