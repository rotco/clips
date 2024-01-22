from typing import List, Optional, Union

import logging

import boto3


class ClipsHelper:
    def __init__(self, database: str = None) -> None:
        self.database = database
        self.client = None
        self.headers = None
        self.selected_vehicle_types = None
        self.limit_rows = None
        self.query = None
        self.query_timeout = None
        self.selected_clip_names = None
        self.stats_interval = 10
        self.stats_count = 10

    def init_client(self) -> boto3.client:
        self.client = boto3.client('athena')
        return self.client

    def set_selected_vehicle_type(self, types: List[str]) -> None:
        self.selected_vehicle_types = types

    # def set_selected_clips(self, clips):
    #     pass

    def set_limit_rows(self, limit_rows: int) -> None:
        if not isinstance(limit_rows, int):
            raise Exception("limit_rows value must be an integer")
        self.limit_rows = limit_rows

    def exec_query(self) -> str:
        res = self.client.start_query_execution(
            QueryString=self.query,
            QueryExecutionContext={
                'Database': self.database
            },
            ResultConfiguration={
                'OutputLocation': 's3://oren-clips-output/'
            }
        )
        return res.get('QueryExecutionId')

    def get_query_results(self, execution_id: str) -> dict:
        return self.client.get_query_results(QueryExecutionId=execution_id)


    def render_list_to_string(self, list: List[str]) -> str:
        text = ''
        for item in list:
            text += f"'{item.lower()}', "
        return text.strip(', ')

    def render_by_intervals(self, interval: int, count: int) -> str:
        if not isinstance(interval, int) or not isinstance(count, int):
            raise Exception("Both interval and count values should be integers")
        text = ''
        try:
            for i in range(count):
                row = '''100 * SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN CAST(detection AS INT) ELSE 0 END) /
                CASE WHEN SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN 1 ELSE 0 END) = 0 THEN 1 ELSE SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN 1 ELSE 0 END) END AS "{start}-{end}",\n
                '''.format(start=i * interval, end=i * interval + interval)
                text += row
        except ZeroDivisionError as e:
            err_msg = "Interval could not be zero"
            logging.error(f"{err_msg}, error: {e}")
            raise Exception(err_msg)

        return text.strip().strip(',')

    def render_query(self) -> None:
        rendered_text_by_intervals = self.render_by_intervals(self.stats_interval, self.stats_count)

        vehicle_type_section = ''
        if self.selected_vehicle_types and isinstance(self.selected_vehicle_types, list):
            vehicle_type_section = f"LOWER(vehicle_type) IN ({self.render_list_to_string(self.selected_vehicle_types)})"

        clip_names_section = ''
        if self.selected_clip_names and isinstance(self.selected_clip_names, list):
            clip_names_section = f"LOWER(clip_name) IN ({self.render_list_to_string(self.selected_clip_names)})"

        and_operator = ''
        if vehicle_type_section and clip_names_section:
            and_operator = "AND"

        where_statement = ''
        if vehicle_type_section or clip_names_section:
            where_statement = "WHERE"


        query = '''SELECT
        vehicle_type,
        {rendered_text_by_intervals}
        FROM oren_parqs
        {where_statement}
        {vehicle_type_section}
        {and_operator}
        {clip_names_section}
        GROUP BY vehicle_type
        LIMIT {limit_rows}
        ;'''.format(
            where_statement=where_statement,
            vehicle_type_section=vehicle_type_section,
            and_operator=and_operator,
            rendered_text_by_intervals=rendered_text_by_intervals,
            clip_names_section=clip_names_section,
            limit_rows=self.limit_rows
        )
        self.query = query


if __name__ == '__main__':
    clip = ClipsHelper(database='oren-clips-output')
    clip.init_client()
    clip.set_limit_rows(10)
    clip.selected_clip_names = ['PNT1_Det4_210125_152642_0000_s001_v_AllCams_s60_0010']
    # clip.set_selected_vehicle_type(['scooter'])
    clip.render_query()
    execution_id = clip.exec_query()
    # execution_id = '6b5c2ffa-cbad-43c4-a75f-a98f3ba106e8'
    results = clip.get_query_results(execution_id)
    print(results)

# docstring
# typehinting
# documentation
