from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_query - """
                 INSERT INTO public.songplays
                {}
                """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.query = query

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadFactOperator.insert_query.format(self.query)
        self.log.info('Start running inserting with query {}'.format(formatted_sql))
        redshift_hook.run(formatted_sql)
