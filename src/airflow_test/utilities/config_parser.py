"""
This module contains parsers which will automatically parse queries based on
configuration files.
"""
#  pylint: disable=duplicate-string-formatting-argument
#  pylint: disable=unidiomatic-typecheck
#  pylint: disable=unused-argument
#  pylint: disable=dangerous-default-value
import logging
from functools import reduce

from pyspark import sql
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from airflow_test.utilities.spark_util import get_spark_session


# Query generator class
class QueryGenerator:

    """
    Purpose: This class is used to generate the queries from configurations.
             It accepts table_name as string, table_params as dict
    """

    @staticmethod
    def aggregate(table_name, table_params, column_function, **kwargs):
        """Function to create query from config. file parameters"""
        try:

            feature_list = table_params["feature_list"]

            features = column_function(feature_list, **kwargs)

            event_date_column = table_params.get("event_date_column")

            if event_date_column is not None:
                QueryGenerator.__add_start_of_week(features, event_date_column)
                QueryGenerator.__add_start_of_month(features, event_date_column)
                QueryGenerator.__add_event_partition_date(features, event_date_column)

            # if don't want to use where clause then put empty string "" in query_parameters.yaml
            where_clause = table_params.get("where_clause", "")

            # if features are not listed we can assume it to be *
            # or can raise a exception
            projection = ",".join(features) if len(features) != 0 else "*"

            # if don't want to use group by then put empty string "" in query_parameters.yaml

            granularity = table_params["granularity"]

            if granularity != "":
                query = "Select {},{} from {} {} group by {}".format(
                    granularity, projection, table_name, where_clause, granularity
                )
            else:
                query = "Select {} from {} {}".format(
                    projection, table_name, where_clause
                )

            logging.info("SQL QUERY {}".format(query))

            return query

        except Exception as e:  # pylint: disable=broad-except
            print(str(e))
            print("Table parameters are missing.")

    @staticmethod
    def __add_event_partition_date(feature_list, event_date_column):
        """Helper function to add daily level partition date column"""
        feature_list.append(
            "date({}) as event_partition_date".format(event_date_column)
        )

    @staticmethod
    def __add_start_of_week(feature_list, event_date_column):
        """Helper function to add weekly level partition date column"""
        feature_list.append(
            "date(date_trunc('week', {})) as start_of_week".format(event_date_column)
        )

    @staticmethod
    def __add_start_of_month(feature_list, event_date_column):
        """Helper function to add monthly level partition date column"""
        feature_list.append(
            "date(date_trunc('month', {})) as start_of_month".format(event_date_column)
        )

    @staticmethod
    def normal_feature_listing(feature_list, **kwargs):
        """Helper function to create key value pairs for query parser"""
        features = []

        for key, val in feature_list.items():
            features.append("{} as {}".format(val, key))

        return features

    @staticmethod
    def expansion_feature_listing(feature_list, **kwargs):
        """Helper function to create key value pairs for query parser extender"""
        features = []

        for key, val in feature_list.items():
            for col in val:
                features.append("{}({}) as {}".format(key, col, col + "_" + key))

        return features


def node_from_config(input_df: DataFrame, config: dict) -> DataFrame:
    """
    Purpose: This is used to automatically generate features using configurations
    :param input_df:
    :param config:
    :return:
    """
    if len(input_df.head(1)) == 0:
        return input_df

    for col in input_df.columns:
        input_df = input_df.withColumnRenamed(
            col, col.strip("\r").strip("\n").strip("\t").strip(" ").replace(".", "")
        )

    for col_value in input_df.columns:
        input_df = input_df.withColumn(
            col_value,
            F.trim(
                F.regexp_replace(F.regexp_replace(F.col(col_value), "\r", ""), "\n", "")
            ),
        )

    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)

    sql_stmt = QueryGenerator.aggregate(
        table_name=table_name,
        table_params=config,
        column_function=QueryGenerator.normal_feature_listing,
    )

    spark = get_spark_session()

    df = spark.sql(sql_stmt)
    return df


def remove_duplicate(input_df: sql.DataFrame) -> sql.DataFrame:
    """
    Function to remove exact duplicate records
    """
    input_df = input_df.drop_duplicates()

    return input_df


def clean_column_value_and_duplicates(
    input_df: sql.DataFrame, lst_of_cols: list = []
) -> sql.DataFrame:
    """Function for removing duplicate rows and cleaning extra spaces
    in string columns

        Args:
            input_df(sql.DataFrame): Input Dataframe
            lst_of_cols(list): List of columns which needs to be cleaned.

        Returns:
            Dataframe with cleaned column values
    """
    if len(input_df.head(1)) == 0:
        return input_df

    for column, type_of_col in input_df.dtypes:
        if type_of_col == "string":
            input_df = input_df.withColumn(
                column,
                F.when(F.col(column) == "\\N", None).otherwise(F.col(column)),
            ).withColumn(column, F.trim(F.col(column)))

            if lst_of_cols:
                for column_val in lst_of_cols:
                    input_df = input_df.withColumn(
                        column_val,
                        F.trim(
                            F.regexp_replace(
                                F.regexp_replace(
                                    F.regexp_replace(
                                        F.trim(F.col(column_val)), " ", ""
                                    ),
                                    "\r",
                                    "",
                                ),
                                "\n",
                                "",
                            )
                        ),
                    )

    return input_df


def filter_null_rows(
    df: sql.DataFrame, columns: list
) -> [sql.DataFrame, sql.DataFrame]:
    """Function to drop all the rows which contains null in the input
    list of column(s)

        Args:
            df(sql.DataFrame): Input Dataframe
            columns(list): List of column(s) which needs to be checked
            for null values.

        Returns:
            Dataframe with no null values in the input list of column(s).
    """

    if len(df.head(1)) == 0:
        return [df, df]

    tot_rejected_rows = None
    for col_name in columns:
        rejected_df = df.filter(df[col_name].isNull())
        df = df.filter(df[col_name].isNotNull())
        if not tot_rejected_rows:
            tot_rejected_rows = rejected_df
        else:
            tot_rejected_rows = tot_rejected_rows.union(
                rejected_df.select(tot_rejected_rows.columns)
            ).distinct()

    tot_rejected_rows = tot_rejected_rows.withColumn(
        "reject_reason", F.lit("primary_key_column_null_check")
    )

    return [df, tot_rejected_rows]


def apply_primary_key_constraints(
    df: sql.DataFrame, columns: list, orderby_col="load_date"
) -> [sql.DataFrame, sql.DataFrame]:
    """Function for removing null value and pickup latest value
    from the partitioned data based on input list of column. Latest row
    is picked on the basis of "snapshot_as_on" column.

    Args:
        df(sql.DataFrame): Input Dataframe
        columns(list): List of column(s) which needs to be checked
        for null values and partitioning data.
        orderby_col: order by column for ordering data in order to find the top record.

    Returns:
        Dataframe with latest value for a particular unique value
        of input list of columns.
    """
    if len(df.head(1)) == 0:
        return [df, df]

    null_removal_df, null_rejected_df = filter_null_rows(df, columns)

    window = Window.partitionBy(columns).orderBy(F.desc(orderby_col))

    df = (
        null_removal_df.withColumn("rownum", F.row_number().over(window))
        .where("rownum=1")
        .drop("rownum")
    )

    rejected_df = (
        null_removal_df.withColumn("rownum", F.row_number().over(window))
        .where("rownum>1")
        .drop("rownum")
    )
    rejected_df = rejected_df.withColumn(
        "reject_reason", F.lit("duplicate_primary_key_check")
    )

    tot_rejected_df = null_rejected_df.union(
        rejected_df.select(null_rejected_df.columns)
    )

    return [df, tot_rejected_df]


def copy_data(df: sql.DataFrame) -> sql.DataFrame:
    """Function for copy dataframe as it is

    Args:
        df(sql.DataFrame): Input Dataframe

    Returns:
        Dataframe without any changes
    """

    return df
