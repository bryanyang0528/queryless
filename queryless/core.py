
import logging

from queryless.adaptor import bigquery as bq


class SQLClient(object):
    def __init__(self, sql_engine: str, **kwargs):
        sql_engine = sql_engine.lower()
        if sql_engine in ['bigquery', 'bq']:
            self._client =  bq.BigQuery(kwargs.get('project'))

    def create_table(self, path, table_from):
        self._client.create_table(path=path, table_from=table_from)
