from typing import List, Optional, Union
from athena import AthenaHelper
from utils import int_check, render_item_list_to_string


class ClipsAnalyzer:
    def __init__(self) -> None:
        self.selected_vehicle_types = None
        self.limit_rows = None
        self.query = None
        self.selected_clip_names = None
        self.stats_interval = 10
        self.stats_count = 10

    def set_selected_vehicle_type(self, types: List[str]) -> None:
        self.selected_vehicle_types = types

    def set_selected_clips(self, clips):
        self.selected_clip_names = clips

    def set_stats_interval(self, stats_interval: int) -> None:
        int_check(stats_interval, 'stats_interval')
        self.stats_interval = stats_interval

    def set_stats_count(self, stats_count: int) -> None:
        int_check(stats_count, 'stats_count')
        self.stats_count = stats_count

    def set_limit_rows(self, limit_rows: int) -> None:
        int_check(limit_rows, 'limit_rows')
        self.limit_rows = limit_rows



    def render_by_intervals(self) -> str:
        text = ''
        try:
            for i in range(self.stats_count):
                row = '''100 * SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN CAST(detection AS INT) ELSE 0 END) /
                CASE WHEN SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN 1 ELSE 0 END) = 0 THEN 1 ELSE SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN 1 ELSE 0 END) END AS "{start}-{end}",\n
                '''.format(start=i * self.stats_interval, end=i * self.stats_interval + self.stats_interval)
                text += row
        except ZeroDivisionError as e:
            raise ValueError("Interval could not be zero")

        return text.strip().strip(',')

    def render_query(self) -> None:
        rendered_text_by_intervals = self.render_by_intervals()

        vehicle_type_section = ''
        if self.selected_vehicle_types and isinstance(self.selected_vehicle_types, list):
            vehicle_type_section = f"LOWER(vehicle_type) IN ({render_item_list_to_string(self.selected_vehicle_types)})"

        clip_names_section = ''
        if self.selected_clip_names and isinstance(self.selected_clip_names, list):
            clip_names_section = f"LOWER(clip_name) IN ({render_item_list_to_string(self.selected_clip_names)})"

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
    ath = AthenaHelper(database='oren-clips-output', output_location='s3://oren-output-location/')
    ath.init_client()
    ca = ClipsAnalyzer()
    ca.set_limit_rows(10)
    ca.set_selected_clips(['PNT1_Det4_210125_152642_0000_s001_v_AllCams_s60_0010'])
    ca.set_selected_vehicle_type(['scooter'])
    ca.render_query()
    execution_id = ath.exec_query(ca.query)
    state = ath.wait_for_execution_completion(execution_id)
    if state != 'SUCCEEDED':
        raise Exception(f"Could complete the Query Execution, state: {state}")
    results = ath.get_query_results(execution_id)
    print(results)

# docstring
# typehinting
# documentation
# validate schema version
# get schema metadata
