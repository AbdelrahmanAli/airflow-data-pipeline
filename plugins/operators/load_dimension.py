from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insertion_query_sql = """ INSERT INTO {} {} """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 sql = "",
                 overwrite = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql = sql
        self.overwrite = overwrite

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.overwrite:
            self.log.info("Clearing data from destination Redshift table ...")
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Running query on table "+self.table+" ... ")
        redshift.run(LoadDimensionOperator.insertion_query_sql.format(self.table,self.sql))
