from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
                 INSERT INTO {0}
                 {1};
                 """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 query,
                 trucated_table,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.trucated_table = trucated_table

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.trucated_table is True:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info("Run query for {} table".format(self.table))
        
        format_sql = LoadDimensionOperator.insert_sql.format(self.table, self.query)
        redshift.run(format_sql)
