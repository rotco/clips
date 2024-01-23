import logging
from typing import List
from athena import AthenaHelper
from utils import (
    int_check,
    render_item_list_to_string,
    check_execution_state,
    validation_check,
)


class ClipsAnalyzer:
    """
    ClipsAnalyzer class for analyzing clips using Athena queries.

    Attributes:
    - athena_client: Athena client instance for interacting with Athena.
    - table_name: Name of the table in the Athena database.
    - selected_vehicle_types: List of selected vehicle types for filtering.
    - limit_rows: Limit on the number of rows in the query result.
    - selected_clip_names: List of selected clip names for filtering.
    - stats_interval: Interval for statistical calculations in the query.
    - stats_count: Number of statistical intervals in the query.


    Methods:
    - set_selected_vehicle_type(types: List[str]) -> None:
        Set the selected vehicle types for filtering.

    - set_selected_clips(clips: List[str]) -> None:
        Set the selected clip names for filtering.

    - set_stats_interval(stats_interval: int) -> None:
        Set the interval for statistical calculations.

    - set_stats_count(stats_count: int) -> None:
        Set the number of statistical intervals.

    - set_limit_rows(limit_rows: int) -> None:
        Set the limit on the number of rows in the query result.

    - render_by_intervals() -> str:
        Generate the text for statistical intervals in the query.

    - get_table_scheme() -> dict:
        Retrieve the schema of the specified Athena table.

    - is_scheme_validated(expected_schema: dict) -> bool:
        Check if the existing schema matches the expected schema.

    - render_query() -> None:
        Generate the complete Athena SQL query based on the set parameters.

    - run_query() -> dict:
        Execute the generated Athena query and retrieve the results.

    """
    def __init__(self) -> None:
        self.athena_client = None
        self.table_name = None
        self.selected_vehicle_types = None
        self.limit_rows = None
        self.query = None
        self.selected_clip_names = None
        self.stats_interval = 10
        self.stats_count = 10

    def set_selected_vehicle_type(self, types: List[str]) -> None:
        """
        Set the selected vehicle types for filtering.

        Parameters:
        - types (List[str]): List of vehicle types to be selected.
        """
        self.selected_vehicle_types = types

    def set_selected_clips(self, clips: List[str]) -> None:
        """
        Set the selected clip names for filtering.

        Parameters:
        - clips (List[str]): List of clip names to be selected.
        """
        self.selected_clip_names = clips

    def set_stats_interval(self, stats_interval: int) -> None:
        """
        Set the interval for statistical calculations.

        Parameters:
        - stats_interval (int): Interval for statistical calculations.
        """
        int_check(stats_interval, "stats_interval")
        self.stats_interval = stats_interval

    def set_stats_count(self, stats_count: int) -> None:
        """
        Set the number of statistical intervals.

        Parameters:
        - stats_count (int): Number of statistical intervals.
        """
        int_check(stats_count, "stats_count")
        self.stats_count = stats_count

    def set_limit_rows(self, limit_rows: int) -> None:
        """
        Set the limit on the number of rows in the query result.

        Parameters:
        - limit_rows (int): Limit on the number of rows.
        """
        int_check(limit_rows, "limit_rows")
        self.limit_rows = limit_rows

    def render_by_intervals(self) -> str:
        """
         Rendering the required text according to the statistical intervals / count in the query.

         Returns:
         - str: Text representing statistical intervals in the query.
         """
        text = ""
        try:
            for i in range(self.stats_count):
                row = """100 * SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN CAST(detection AS INT) ELSE 0 END) /
                CASE WHEN SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN 1 ELSE 0 END) = 0 THEN 1 ELSE SUM(CASE WHEN distance BETWEEN {start} AND {end} THEN 1 ELSE 0 END) END AS "{start}-{end}",\n
                """.format(
                    start=i * self.stats_interval,
                    end=i * self.stats_interval + self.stats_interval,
                )
                text += row
        except ZeroDivisionError as e:
            raise ValueError("Interval could not be zero")

        return text.strip().strip(",")

    def get_table_scheme(self) -> dict:
        """
        Retrieve the schema of the specified Athena table.

        Returns:
        - dict: Dictionary representing the table schema.
        """
        query = f"SELECT column_name, data_type FROM INFORMATION_SCHEMA.columns where table_name='{self.table_name}';"
        execution_id = self.athena_client.exec_query(query)
        check_execution_state(
            ca.athena_client.wait_for_execution_completion(execution_id)
        )
        results = ca.athena_client.get_query_results(execution_id)
        try:
            return results["ResultSet"]["Rows"]
        except KeyError as e:
            logging.error(
                f"Unexpected object was returned while getting results for get_table_scheme, results: {results}"
            )
            raise KeyError(e)

    def is_scheme_validated(self, expected_schema: dict):
        """
        Check if the existing schema matches the expected schema.

        Parameters:
        - expected_schema (dict): Expected table schema.

        Returns:
        - bool: True if the schema is validated, False otherwise.
        """
        existing_schema_rows = self.get_table_scheme()
        validated = True
        # ignoring the first row, which are the headers
        for i in range(1, len(existing_schema_rows)):
            try:
                key = existing_schema_rows[i]["Data"][0]["VarCharValue"]
                existing_value = existing_schema_rows[i]["Data"][1]["VarCharValue"]
            except KeyError as e:
                logging.error(
                    f"Unexpected object was returned while parsing schema row, row: {existing_schema_rows[i]}"
                )
                raise KeyError(e)
            expected_value = expected_schema.get(key)
            if expected_value and expected_value != existing_value:
                validated = False
                logging.warning(
                    f"Found Schema validation error, for name: '{key}' expected type: '{expected_value}',found type: '{existing_value}'"
                )

        return validated

    def render_query(self) -> None:
        """
         Render the complete Athena SQL query based on the set parameters.
         """
        rendered_text_by_intervals = self.render_by_intervals()
        vehicle_type_section = ""
        if self.selected_vehicle_types and isinstance(
            self.selected_vehicle_types, list
        ):
            vehicle_type_section = f"LOWER(vehicle_type) IN ({render_item_list_to_string(self.selected_vehicle_types)})"

        clip_names_section = ""
        if self.selected_clip_names and isinstance(self.selected_clip_names, list):
            clip_names_section = f"LOWER(clip_name) IN ({render_item_list_to_string(self.selected_clip_names)})"

        and_operator = ""
        if vehicle_type_section and clip_names_section:
            and_operator = "AND"

        where_statement = ""
        if vehicle_type_section or clip_names_section:
            where_statement = "WHERE"

        query = """SELECT
        vehicle_type,
        {rendered_text_by_intervals}
        FROM {table_name}
        {where_statement}
        {vehicle_type_section}
        {and_operator}
        {clip_names_section}
        GROUP BY vehicle_type
        LIMIT {limit_rows}
        ;""".format(
            table_name=self.table_name,
            where_statement=where_statement,
            vehicle_type_section=vehicle_type_section,
            and_operator=and_operator,
            rendered_text_by_intervals=rendered_text_by_intervals,
            clip_names_section=clip_names_section,
            limit_rows=self.limit_rows,
        )
        self.query = query

    def run_query(self) -> dict:
        """
        Execute the generated Athena query and retrieve the results.

        Returns:
        - dict: Query results in dictionary format.
        """
        execution_id = self.athena_client.exec_query(ca.query)
        check_execution_state(
            self.athena_client.wait_for_execution_completion(execution_id)
        )
        return ca.athena_client.get_query_results(execution_id)


if __name__ == "__main__":
    ca = ClipsAnalyzer()
    ca.athena_client = AthenaHelper(
        database="oren-clips-output", output_location="s3://oren-output-location/"
    )
    ca.athena_client.init_client()
    ca.table_name = "oren_parqs"
    validated = ca.is_scheme_validated({"clip_name": "varchar", "detection": "boolean"})
    validation_check(validated, "is_scheme_validated")
    ca.set_limit_rows(10)
    ca.set_stats_count(5)
    ca.set_stats_interval(20)
    ca.set_selected_clips(["PNT1_Det4_210125_152642_0000_s001_v_AllCams_s60_0010"])
    ca.set_selected_vehicle_type(["scooter", "bus"])
    ca.render_query()
    results = ca.run_query()
    print(results)

