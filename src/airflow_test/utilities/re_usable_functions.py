"""
This module contains reusable functions that can be used anywhere in the project.
"""
#  pylint: disable=duplicate-string-formatting-argument
#  pylint: disable=unidiomatic-typecheck
import logging
import os
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from airflow_test.utilities.spark_util import get_spark_session

conf = os.getenv("CONF", "base")
log = logging.getLogger(__name__)


def gen_max_sql(data_frame, table_name, group):
    """
    Purpose: To get the max values of columns via SQL
    :param data_frame:
    :param table_name:
    :param group:
    :return:
    """
    grp_str = ", ".join(group)
    col_to_iterate = [
        "max(" + x + ")" + " as " + x for x in data_frame.columns if x not in group
    ]
    all_cols = ", ".join(col_to_iterate)
    final_str = "select {0}, {1} {2} {3} group by {4}".format(
        grp_str, all_cols, "from", table_name, grp_str
    )
    return final_str


def union_dataframes_with_missing_cols(df_input_or_list, *args):
    """
    Purpose: To perform union of multiple dataframes(homogeneous/ heterogeneous)
    :param df_input_or_list:
    :param args:
    :return:
    """
    if type(df_input_or_list) is list:
        df_list = df_input_or_list
    elif type(df_input_or_list) is DataFrame:
        df_list = [df_input_or_list] + list(args)

    col_list = set()
    for df in df_list:
        for column in df.columns:
            col_list.add(column)

    def add_missing_cols(dataframe, col_list):
        """
        This helper function adds missing columns in heterogeneous input dataset
        """
        missing_cols = [
            columns for columns in col_list if columns not in dataframe.columns
        ]
        for columns in missing_cols:
            dataframe = dataframe.withColumn(columns, F.lit(None))
        return dataframe  # dataframe.select(*sorted(col_list))

    df_list_updated = [add_missing_cols(df, col_list) for df in df_list]
    return reduce(DataFrame.unionByName, df_list_updated)


def execute_sql(data_frame, table_name, sql_str):
    """
    Purpose: To execute the sql statements
    :param data_frame:
    :param table_name:
    :param sql_str:
    :return:
    """
    ss = get_spark_session()
    data_frame.registerTempTable(table_name)
    return ss.sql(sql_str)


def add_start_of_week_and_month(input_df, date_column="day_id"):
    """
    Purpose: To generate date partition columns from input date column
    :param input_df:
    :param date_column:
    :return:
    """

    if len(input_df.head(1)) == 0:
        return input_df

    input_df = (
        input_df.withColumn(
            "start_of_week", F.to_date(F.date_trunc("week", F.col(date_column)))
        )
        .withColumn(
            "start_of_month", F.to_date(F.date_trunc("month", F.col(date_column)))
        )
        .withColumn("event_partition_date", F.to_date(F.col(date_column)))
    )

    return input_df


def add_event_week_and_month_from_yyyymmdd(
    input_df: DataFrame, column: str
) -> DataFrame:
    """
    Purpose: To generate date partition columns from input date column from yyyyMMdd format
    :param input_df:
    :param column:
    :return:
    """
    input_col = column
    event_partition_date = "event_partition_date"
    input_df = input_df.withColumn(
        "event_partition_date",
        F.to_date(F.col(input_col).cast(StringType()), "yyyyMMdd"),
    )
    input_df = input_df.withColumn(
        "start_of_week", F.to_date(F.date_trunc("week", F.col(event_partition_date)))
    )
    input_df = input_df.withColumn(
        "start_of_month", F.to_date(F.date_trunc("month", F.col(event_partition_date)))
    )

    return input_df


def __divide_chunks(l, n):  # NOQA # pylint: disable=missing-yield-type-doc
    """
    This function is used to divide a huge list into smaller chunks.
    :param l: length of list
    :param n: number of chunks values in each chunk
    :return: small lists
    """
    for i in range(0, len(l), n):
        yield l[i : i + n]


def __is_valid_input_df(input_df, cust_profile_df):
    """
    Valid input criteria:
    1. input_df is provided and it is not empty
    2. cust_profile_df is either:
        - provided with non empty data OR
        - not provided at all
    """
    return (input_df is not None and len(input_df.head(1)) > 0) and (
        cust_profile_df is None or len(cust_profile_df.head(1)) > 0
    )
