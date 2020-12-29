from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 tests = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        issuesCount = 0
        for test in self.tests:
            testQuery = test['test']
            testExpectedResult = int(test['expected'])
            records = redshift.get_records(testQuery)
            if len(records) < 1 or len(records[0]) < 1 or recprds[0][0]!=testExpectedResult:
                issuesCount++
        
        if issuesCount > 0:
            raise ValueError(f"There are {issuesCount} data quality issues.")
            
        logging.info(f"Data quality check passed sucessfully")

            