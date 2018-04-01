from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SchemaField

from queryless.parser import BasicParser


class BigQuery(object):

    def __init__(self, project=None):
        self._client = bigquery.Client(project=project)

    @property
    def client(self):
        return self._client

    def create_table(self, path, table_from='uri'):
        bp = BQParser(path)
        dataset_name = bp.dataset_name
        table_name = bp.table_name
        skip_leading_rows = bp.skip_leading_rows
        schema = bp.schema

        table_ref = self.client.dataset(dataset_name).table(table_name)
        load_config = LoadJobConfig()
        load_config.skip_leading_rows = skip_leading_rows
        load_config.schema = schema
        file_source = bp.properties.get('inputPath')

        if table_from == 'uri':
            self.client.load_table_from_uri(source_uris=file_source,
                                            destination=table_ref,
                                            job_config=load_config)
        else:
            raise ValueError('Not supported')


class BQParser(BasicParser):

    def __init__(self, path: str):
        super().__init__(path=path)

    @property
    def dataset_name(self) -> str:
        return self.metadata.get('datasetName')

    @property
    def table_name(self) -> str:
        return self.metadata.get('tableName')

    @property
    def properties(self) -> dict:
        return self.metadata.get('srcProperty')

    @property
    def skip_leading_rows(self) -> int:
        return self.metadata.get('skipLeadingRows', 0)

    @property
    def schema(self) -> list:
        """
        SCHEMA = [
            SchemaField('full_name', 'STRING', mode='required'),
            SchemaField('age', 'INTEGER', mode='required'),
        ]
        :return: a list
        """

        schema = self.spec.get('schema')

        schema = [[SchemaField(k, i[k]['type'], i[k]['mode']) for k in i.keys()] for i in schema]
        schema = [item for sublist in schema for item in sublist]
        return schema





