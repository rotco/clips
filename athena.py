from typing import List, Optional, Union

import logging
import time
import boto3


class AthenaHelper:
    def __init__(self, database: str = None, output_location: str = None) -> None:
        self.database = database
        self.output_location = output_location
        self.client = None
        self.query = None

    def init_client(self) -> boto3.client:
        self.client = boto3.client('athena')
        return self.client

    def exec_query(self, query) -> str:
        res = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.database
            },
            ResultConfiguration={
                'OutputLocation': self.output_location
            }
        )
        return res.get('QueryExecutionId')

    def wait_for_execution_completion(self, query_execution_id, retries_limit=100):
        retries_count = 0
        while retries_limit >= retries_count:
            result = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            state = result['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'CANCELLED', 'FAILED']:
                return state
            time.sleep(2)
            retries_count += 1
        return "TIMEOUT"


    def get_query_results(self, execution_id: str) -> dict:
        return self.client.get_query_results(QueryExecutionId=execution_id)



# docstring
# typehinting
# documentation
# separting clips and athena classes
# jinja2
# schema validating
