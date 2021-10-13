"""This module contains base IO class to implement core input-output functionalities like
incremental load, latest load , incremental save etc."""
import logging
# pylint: disable=invalid-name
import pickle
from copy import deepcopy
from fnmatch import fnmatch
from functools import partial
from pathlib import Path, PurePosixPath, WindowsPath
from typing import Any, Dict, List, Tuple
from warnings import warn

from hdfs import HdfsError, InsecureClient
from kedro.extras.datasets.spark.spark_dataset import _dbfs_glob
from kedro.io import AbstractVersionedDataSet, Version
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException
from s3fs import S3FileSystem

from airflow_test.utilities.re_usable_functions import (
    union_dataframes_with_missing_cols,
)
from airflow_test.utilities.spark_util import get_spark_empty_df, get_spark_session

log = logging.getLogger(__name__)


def _parse_glob_pattern(pattern: str) -> str:
    """Helper function to parse the patterns"""
    special = ("*", "?", "[")
    clean = []
    for part in pattern.split("/"):
        if any(char in part for char in special):
            break
        clean.append(part)
    return "/".join(clean)


def _split_filepath(filepath: str) -> Tuple[str, str]:
    """Helper function to split the input filepath"""
    split_ = filepath.split("://", 1)
    if len(split_) == 2:
        return split_[0] + "://", split_[1]
    return "", split_[0]


def _strip_dbfs_prefix(path: str) -> str:
    """Helper function to strip filesystem dbfs name from filepath"""
    return path[len("/dbfs") :] if path.startswith("/dbfs") else path


class KedroHdfsInsecureClient(InsecureClient):
    """Subclasses ``hdfs.InsecureClient`` and implements ``hdfs_exists``
    and ``hdfs_glob`` methods required by ``SparkDataSet``"""

    def hdfs_exists(self, hdfs_path: str) -> bool:
        """Determines whether given ``hdfs_path`` exists in HDFS.

        Args:
            hdfs_path: Path to check.

        Returns:
            True if ``hdfs_path`` exists in HDFS, False otherwise.
        """
        return bool(self.status(hdfs_path, strict=False))

    def hdfs_glob(self, pattern: str) -> List[str]:
        """Perform a glob search in HDFS using the provided pattern.

        Args:
            pattern: Glob pattern to search for.

        Returns:
            List of HDFS paths that satisfy the glob pattern.
        """
        prefix = _parse_glob_pattern(pattern) or "/"
        matched = set()
        try:
            for dpath, _, fnames in self.walk(prefix):
                if fnmatch(dpath, pattern):
                    matched.add(dpath)
                matched |= set(
                    "{}/{}".format(dpath, fname)
                    for fname in fnames
                    if fnmatch("{}/{}".format(dpath, fname), pattern)
                )
        except HdfsError:  # pragma: no cover
            # HdfsError is raised by `self.walk()` if prefix does not exist in HDFS.
            # Ignore and return an empty list.
            pass
        return sorted(matched)


def _get_dbutils(self):
    """Helper function to get DBUtils utilities in order to access dbfs file system"""
    try:
        from pyspark.dbutils import DBUtils  # pylint: disable=import-outside-toplevel

        spark = get_spark_session()
        dbutils = DBUtils(spark)

    except ImportError:
        import IPython  # pylint: disable=import-outside-toplevel

        dbutils = IPython.get_ipython().user_ns["dbutils"]

    return dbutils


class SparkDataSet(AbstractVersionedDataSet):
    """``SparkDataSet`` loads and saves Spark dataframes.

    Example:
    ::

        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import (StructField, StringType,
        >>>                                IntegerType, StructType)
        >>>
        >>> from kedro.extras.datasets.spark import SparkDataSet
        >>>
        >>> schema = StructType([StructField("name", StringType(), True),
        >>>                      StructField("age", IntegerType(), True)])
        >>>
        >>> data = [('Alex', 31), ('Bob', 12), ('Clarke', 65), ('Dave', 29)]
        >>>
        >>> spark_df = SparkSession.builder.getOrCreate()\
        >>>                        .createDataFrame(data, schema)
        >>>
        >>> data_set = SparkDataSet(filepath="test_data")
        >>> data_set.save(spark_df)
        >>> reloaded = data_set.load()
        >>>
        >>> reloaded.take(4)
    """

    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filepath: str,
        file_format: str = "parquet",
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        metadata_table_path: str = "",
        credentials: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``SparkDataSet``.

        Args:
            filepath: Filepath in POSIX format to a Spark dataframe. When using Databricks
                and working with data written to mount path points,
                specify ``filepath``s for (versioned) ``SparkDataSet``s
                starting with ``/dbfs/mnt``.
            file_format: File format used during load and save
                operations. These are formats supported by the running
                SparkContext include parquet, csv. For a list of supported
                formats please refer to Apache Spark documentation at
                https://spark.apache.org/docs/latest/sql-programming-guide.html
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find
                a list of read options for each supported format
                in Spark DataFrame read documentation:
                https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
            save_args: Save args passed to Spark DataFrame write options.
                Similar to load_args this is dependent on the selected file
                format. You can pass ``mode`` and ``partitionBy`` to specify
                your overwrite mode and partitioning respectively. You can find
                a list of options for each format in Spark DataFrame
                write documentation:
                https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials to access the S3 bucket, such as
                ``key``, ``secret``, if ``filepath`` prefix is ``s3a://`` or ``s3n://``.
                Optional keyword arguments passed to ``hdfs.client.InsecureClient``
                if ``filepath`` prefix is ``hdfs://``. Ignored otherwise.
        """
        credentials = deepcopy(credentials) or {}
        fs_prefix, filepath = _split_filepath(filepath)
        exists_function = None
        glob_function = None

        if fs_prefix in ("s3a://", "s3n://"):
            if fs_prefix == "s3n://":
                warn(
                    "`s3n` filesystem has now been deprecated by Spark, "
                    "please consider switching to `s3a`",
                    DeprecationWarning,
                )
            _s3 = S3FileSystem(**credentials)
            exists_function = _s3.exists
            glob_function = partial(_s3.glob, refresh=True)
            path = PurePosixPath(filepath)

        elif fs_prefix == "hdfs://" and version:
            warn(
                "HDFS filesystem support for versioned {} is in beta and uses "
                "`hdfs.client.InsecureClient`, please use with caution".format(
                    self.__class__.__name__
                )
            )

            # default namenode address
            credentials.setdefault("url", "http://localhost:9870")
            credentials.setdefault("user", "hadoop")

            _hdfs_client = KedroHdfsInsecureClient(**credentials)
            exists_function = _hdfs_client.hdfs_exists
            glob_function = _hdfs_client.hdfs_glob  # type: ignore
            path = PurePosixPath(filepath)

        else:
            path = filepath
            if filepath.startswith("/dbfs"):
                dbutils = _get_dbutils(self._get_spark())
                if dbutils:
                    glob_function = partial(_dbfs_glob, dbutils=dbutils)
        # else:
        #     exists_function = glob_function = None  # type: ignore
        #     path = Path(filepath)  # type: ignore
        #

        super().__init__(
            filepath=path,  # MISSING lOAD AND save ARGS
            version=version,
            exists_function=exists_function,
            glob_function=glob_function,
        )

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._file_format = file_format
        self._fs_prefix = fs_prefix

        self._process_type_load = (
            load_args.get("process_type", None) if load_args is not None else None
        )
        self._process_type_save = (
            save_args.get("process_type", None) if save_args is not None else None
        )
        self._read_layer = (
            load_args.get("read_layer", None) if load_args is not None else None
        )
        self._target_layer = (
            load_args.get("target_layer", None) if load_args is not None else None
        )
        self._lookback = (
            load_args.get("lookback", None) if load_args is not None else None
        )
        self._lookup_table_name = (
            load_args.get("lookup_table_name", None) if load_args is not None else None
        )

        self._delimiter = (
            load_args.get("delimiter", ",") if load_args is not None else None
        )

        self._escape = load_args.get("escape", None) if load_args is not None else None

        self._read_layer_save = (
            save_args.get("read_layer", None) if save_args is not None else None
        )
        self._target_layer_save = (
            save_args.get("target_layer", None) if save_args is not None else None
        )

        self._metadata_table_path = (
            metadata_table_path
            if (metadata_table_path is not None and metadata_table_path.endswith("/"))
            else metadata_table_path + "/"
        )

        self._partitionBy = (
            save_args.get("partitionBy", None) if save_args is not None else None
        )
        self._mode = save_args.get("mode", None) if save_args is not None else None
        self._mergeSchema = (
            load_args.get("mergeSchema", None) if load_args is not None else None
        )

    def __getstate__(self):
        # SparkDataSet cannot be used with ParallelRunner
        raise AttributeError(f"{self.__class__.__name__} cannot be serialized!")

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._fs_prefix + str(self._filepath),
            file_format=self._file_format,
            load_args=self._load_args,
            save_args=self._save_args,
            version=self._version,
        )

    @staticmethod
    def _get_spark():
        """Static helper function to get spark session"""
        spark = get_spark_session()
        return spark

    def _get_dbutils(self):
        """Helper function to get DBUtils utilities in order to access dbfs file system"""
        try:
            from pyspark.dbutils import (
                DBUtils,
            )  # pylint: disable=import-outside-toplevel

            dbutils = DBUtils(self._get_spark())

        except ImportError:
            import IPython  # pylint: disable=import-outside-toplevel

            dbutils = IPython.get_ipython().user_ns["dbutils"]

        return dbutils

    def _get_raw_source_data(
        self,
        file_path,
        process_type,
        spark,
        dbutils,
        file_format,
        last_load_date=None,
    ):
        """
        function to parse raw layer data based on process type (incremental/ latest)
        :param last_load_date: last data process date for the dataset
        :param process_type: this defines the data processing type (incremental/latest)
        :param spark: spark session
        :param dbutils: function to parse the azure adls paths
        :param file_path: path of the source data file
        :param file_format: format of source data file
        :return: source dataframe
        :raises ValueError:
        """

        schema = StructType(
            [
                StructField("path", StringType(), True),
                StructField("name", StringType(), True),
                StructField("size", StringType(), True),
            ]
        )
        rdd = spark.sparkContext.parallelize(dbutils.fs.ls(file_path))
        df_path = spark.createDataFrame(rdd, schema)
        df_path = (
            df_path.withColumn("path", F.regexp_replace("path", "dbfs:", ""))
            .withColumn(
                "name",
                F.to_date(
                    F.unix_timestamp(
                        F.substring(F.regexp_replace("name", "/", ""), 1, 10),
                        "yyyy_MM_dd",
                    ).cast("timestamp")
                ),
            )
            .drop("size")
        )

        df_path_null = df_path.where("name is null")
        if len(df_path_null.head(1)) == 0:
            log.info("Source directory structure is correct")
        else:
            raise ValueError(
                "Source data is not kept in appropriate date folders with format yyyy_MM_dd_hh_mm_ss"
            )

        if process_type == "incremental":
            path_df_to_process = df_path.where(
                "name is not null and to_date(name,'yyyy-MM-dd') > to_date('{0}', 'yyyy-MM-dd')".format(
                    last_load_date
                )
            ).select("path", "name")
            paths_to_process = path_df_to_process.rdd.collect()
            log.info("paths_to_process: {}".format(paths_to_process))
        elif process_type == "latest":
            path_df_to_process_int = df_path.withColumn(
                "rn", F.expr("row_number() over(order by name desc)")
            )
            path_df_to_process = path_df_to_process_int.where("rn = 1").select(
                "path", "name"
            )
            paths_to_process = path_df_to_process.rdd.collect()

        else:
            log.info(
                "Unknown Process type value.Please check. Returning empty dataframe "
            )
            return get_spark_empty_df()

        if len(paths_to_process) != 0:
            for i, val in enumerate(paths_to_process):
                path = val[0]
                load_date = val[1]

                if i == 0:
                    if file_format == "csv":
                        final_df = (
                            spark.read.option("header", True)
                            .option("delimiter", self._delimiter)
                            .option("quote", '"')
                            .option("escape", self._escape)
                            .option("multiLine", "true")
                            .option("recursiveFileLookup", "true")
                            .csv(path)
                        )
                    elif file_format == "parquet":
                        final_df = spark.read.load(
                            path, self._file_format, **self._load_args
                        )
                    else:
                        log.info("File type is not correct. Please check.")
                    final_df = final_df.withColumn("load_date", F.lit(load_date))
                    for col in final_df.columns:
                        final_df = final_df.withColumnRenamed(
                            col, col.strip("\r").strip("\n").strip("\t").strip(" ")
                        )
                else:
                    if file_format == "csv":
                        int_df = (
                            spark.read.option("header", True)
                            .option("delimiter", self._delimiter)
                            .option("quote", '"')
                            .option("escape", self._escape)
                            .option("multiLine", "true")
                            .option("recursiveFileLookup", "true")
                            .csv(path)
                        )
                    elif file_format == "parquet":
                        int_df = spark.read.load(
                            path, self._file_format, **self._load_args
                        )
                    else:
                        log.info("File type is not correct. Please check.")
                    int_df = int_df.withColumn("load_date", F.lit(load_date))
                    for col in int_df.columns:
                        int_df = int_df.withColumnRenamed(
                            col, col.strip("\r").strip("\n").strip("\t").strip(" ")
                        )
                    final_df = union_dataframes_with_missing_cols(final_df, int_df)
        else:
            final_df = get_spark_empty_df()

        log.info("raw data is created")
        return final_df

    def _create_metadata_table(self, spark):
        """Helper function to create metadata table in case if it doesn't exist
        during 1st time pipeline runs"""

        metadata_table_path = self._metadata_table_path

        df = spark.range(1)

        metadata_table_df = (
            df.withColumn("table_name", F.lit("metadata_table"))
            .withColumn("table_path", F.lit(metadata_table_path))
            .withColumn("write_mode", F.lit("None"))
            .withColumn("process_type", F.lit("None"))
            .withColumn("target_max_data_load_date", F.current_date())
            .withColumn("updated_on", F.current_date())
            .withColumn("read_layer", F.lit("read_layer"))
            .withColumn("target_layer", F.lit("target_layer"))
            .withColumn("record_count", F.lit("0"))
            .drop("id")
        )

        metadata_table_df.write.partitionBy("table_name").format("parquet").mode(
            "append"
        ).save(metadata_table_path)

    def _get_metadata_max_data_date(self, spark, table_name):
        """Helper function to get max data partition date corresponding to a dataset
        from metadata table"""

        metadata_table_path = self._metadata_table_path
        lookup_table_name = table_name

        logging.info("metadata_table_path: {}".format(metadata_table_path))
        try:
            if len(metadata_table_path) == 0 or metadata_table_path is None:
                raise ValueError(
                    "Metadata table path can't be empty in incremental mode"
                )
            else:
                logging.info(
                    "checking whether metadata table exist or not at path : {}".format(
                        metadata_table_path
                    )
                )
                metadata_table = spark.read.parquet(metadata_table_path)

                logging.info(
                    "metadata table exists at path: {}".format(metadata_table_path)
                )

        except AnalysisException as e:
            logging.info("metadata table doesn't exist. Creating new metadata table")
            log.exception("Exception raised", str(e))

            self._create_metadata_table(spark)
            metadata_table = spark.read.parquet(metadata_table_path)

        metadata_table.createOrReplaceTempView("mdtl")

        target_max_load_date = spark.sql(
            """
        select nvl(cast(max(target_max_data_load_date) as string),'1970-01-01') as target_max_data_load_date
                        from mdtl where table_name = '{0}'""".format(
                lookup_table_name
            )
        )

        try:
            if len(target_max_load_date.head(1)) == 0 or target_max_load_date is None:
                log.info(
                    "Max data date of lookup table is None, Setting to default date 1970-01-01"
                )
                target_max_load_date_int = spark.range(1)
                target_max_load_date = target_max_load_date_int.withColumn(
                    "target_max_data_load_date", F.lit("1970-01-01")
                ).drop("id")

        except AnalysisException as e:
            log.exception("Exception raised", str(e))

        return target_max_load_date

    def _get_incremental_data(self):
        """Helper function to get incremental data for processing"""
        try:

            dbutils = self._get_dbutils()
            spark = self._get_spark()

            logging.info("Entered incremental/latest process read mode")
            filepath = self._filepath
            file_format = self._file_format
            read_layer = self._read_layer
            target_layer = self._target_layer
            lookback = self._lookback
            lookup_table_name = self._lookup_table_name
            mergeschema = self._mergeSchema
            process_type = self._process_type_load

            logging.info("filepath: {}".format(filepath))
            logging.info("read_layer: {}".format(read_layer))
            logging.info("target_layer: {}".format(target_layer))
            logging.info("lookback: {}".format(lookback))
            logging.info("mergeSchema: {}".format(mergeschema))
            logging.info("lookup_table_name: {}".format(lookup_table_name))
            logging.info("Fetching source data")
            logging.info("process_type: {}".format(process_type))

            if process_type.lower() == "incremental":
                if lookup_table_name is None or lookup_table_name == "":
                    raise ValueError(
                        "lookup table name can't be empty for incremental load"
                    )
                else:
                    logging.info(
                        "Fetching max data date entry of lookup table from metadata table"
                    )
                    target_max_data_load_date = self._get_metadata_max_data_date(
                        spark, lookup_table_name
                    )
                    tgt_filter_date_temp = target_max_data_load_date.rdd.flatMap(
                        lambda x: x
                    ).collect()
                    if (
                        tgt_filter_date_temp is None
                        or tgt_filter_date_temp == [None]
                        or tgt_filter_date_temp == ["None"]
                    ):
                        raise ValueError(
                            "Please check the return date from _get_metadata_max_data_date function. It can't be empty"
                        )
                    else:
                        tgt_filter_date = "".join(tgt_filter_date_temp)
                        logging.info(
                            "Max data date entry of lookup table in metadata table is: {}".format(
                                tgt_filter_date
                            )
                        )
                if read_layer.lower() == "raw":
                    src_data = self._get_raw_source_data(
                        filepath,
                        process_type,
                        spark,
                        dbutils,
                        file_format,
                        tgt_filter_date,
                    )

                elif (
                    read_layer.lower() == "staging"
                    or read_layer.lower() == "intermediate"
                    or read_layer.lower() == "primary"
                ):
                    filter_col = "load_date"
                    lookback_fltr = (
                        lookback
                        if (
                            (lookback is not None)
                            and (lookback != "")
                            and (lookback != "")
                        )
                        else "0"
                    )
                    print("filter_col:", filter_col)
                    print("lookback_fltr:", lookback_fltr)
                    src_data = spark.read.load(
                        filepath, self._file_format, **self._load_args
                    )
                    src_data.createOrReplaceTempView("src_data")
                    src_data = spark.sql(
                        "select * from src_data where {0} > date_sub(to_date(cast('{1}' as String)) , {2} )".format(
                            filter_col, tgt_filter_date, lookback_fltr
                        )
                    )
                else:
                    log.info("Unknown read layer. Please check")

            elif process_type.lower() == "latest":
                if read_layer.lower() == "raw":
                    src_data = self._get_raw_source_data(
                        filepath, process_type, spark, dbutils, file_format
                    )
                elif (
                    read_layer.lower() == "staging"
                    or read_layer.lower() == "intermediate"
                    or read_layer.lower() == "primary"
                ):
                    src_table_name = filepath.split("/")[-2]
                    if src_table_name is None or src_table_name == "":
                        raise ValueError(
                            "lookup table name can't be empty for incremental load"
                        )
                    else:
                        logging.info(
                            "Fetching max data date entry of source table from metadata table"
                        )
                        src_max_data_load_date = self._get_metadata_max_data_date(
                            spark, src_table_name
                        )
                        src_filter_date_temp = src_max_data_load_date.rdd.flatMap(
                            lambda x: x
                        ).collect()
                        if (
                            src_filter_date_temp is None
                            or src_filter_date_temp == [None]
                            or src_filter_date_temp == ["None"]
                        ):
                            raise ValueError(
                                "Please check the return date from _get_metadata_max_data_date function. "
                                "It can't be empty"
                            )
                        else:
                            src_filter_date = "".join(src_filter_date_temp)
                            logging.info(
                                "Max data date entry of lookup table in metadata table is: {}".format(
                                    src_filter_date
                                )
                            )

                    src_data = spark.read.load(
                        filepath, self._file_format, **self._load_args
                    )
                    src_data.createOrReplaceTempView("src_data")
                    filter_col = "load_date"
                    src_data = spark.sql(
                        "select * from src_data where {0} = to_date(cast('{1}' as String))".format(
                            filter_col, src_filter_date
                        )
                    )

                else:
                    raise ValueError(
                        "Unknown read layer. Please check. "
                        "It can only be among 'raw', 'staging', 'intermediate','primary' "
                    )

            elif process_type.lower() == "full":
                src_data = spark.read.load(
                    filepath, self._file_format, **self._load_args
                )
                src_data.createOrReplaceTempView("src_data")

            else:
                raise ValueError(
                    "Unknown process type. Please check. It can only be among 'incremental', 'latest' "
                )

        except AnalysisException as e:
            log.exception("Exception raised", str(e))

        return src_data

    def _update_metadata_table(
        self,
        spark,
        metadata_table_path,
        target_table_name,
        filepath,
        write_mode,
        process_type,
        file_format,
        partitionby,
        read_layer,
        target_layer,
        rec_cnt,
        mergeschema,
    ):
        """Helper function to update metadata table with relevant operational details
        of a dataset after every run"""

        if mergeschema is not None and mergeschema.lower() == "true":
            current_target_data = (
                spark.read.format(file_format)
                .option("mergeSchema", "true")
                .load(filepath)
            )
        else:
            current_target_data = spark.read.format(file_format).load(filepath)

        current_target_data.createOrReplaceTempView("curr_target")

        current_target_max_data_load_date = spark.sql(
            "select nvl(cast(max({0}) as String),'1970-01-01') as target_max_date from curr_target".format(
                partitionby
            )
        )

        metadata_table_update_max_date_temp = (
            current_target_max_data_load_date.rdd.flatMap(lambda x: x).collect()
        )

        if (
            metadata_table_update_max_date_temp is None
            or metadata_table_update_max_date_temp == [None]
            or metadata_table_update_max_date_temp == ["None"]
            or metadata_table_update_max_date_temp == ""
        ):
            raise ValueError(
                "Please check, the current_target_max_data_load_date can't be empty"
            )
        else:
            metadata_table_update_max_date = "".join(
                metadata_table_update_max_date_temp
            )

        logging.info(
            "Updating control table for {} dataset with date: {} ".format(
                target_table_name, metadata_table_update_max_date
            )
        )

        metadata_table_update_df = spark.range(1)

        metadata_table_update_df = (
            metadata_table_update_df.withColumn("table_name", F.lit(target_table_name))
            .withColumn("table_path", F.lit(filepath))
            .withColumn("write_mode", F.lit(write_mode))
            .withColumn("process_type", F.lit(process_type))
            .withColumn(
                "target_max_data_load_date",
                F.to_date(F.lit(metadata_table_update_max_date), "yyyy-MM-dd"),
            )
            .withColumn("updated_on", F.current_date())
            .withColumn("read_layer", F.lit(read_layer))
            .withColumn("target_layer", F.lit(target_layer))
            .withColumn("record_count", F.lit(rec_cnt))
            .drop("id")
        )

        try:
            metadata_table_update_df.write.partitionBy("table_name").format(
                "parquet"
            ).mode("append").save(metadata_table_path)
        except AnalysisException as e:
            log.exception("Exception raised", str(e))

        logging.info("Metadata table updated for {} dataset".format(target_table_name))

    def _write_incremental_data(self, data):
        """Helper function to write incremental or latest data to storage layer based on process type"""

        logging.info("Entered incremental/latest process write mode")
        spark = self._get_spark()
        filewritepath = self._filepath
        partitionby = self._partitionBy
        mode = self._mode
        file_format = self._file_format
        metadata_table_path = self._metadata_table_path
        read_layer = self._read_layer_save
        target_layer = self._target_layer_save
        target_table_name = filewritepath.split("/")[-2]
        dataframe_to_write = data
        mergeschema = self._mergeSchema
        process_type = self._process_type_save

        logging.info("filewritepath: {}".format(filewritepath))
        logging.info("partitionBy: {}".format(partitionby))
        logging.info("mode: {}".format(mode))
        logging.info("file_format: {}".format(file_format))
        logging.info("metadata_table_path: {}".format(metadata_table_path))
        logging.info("read_layer: {}".format(read_layer))
        logging.info("target_layer: {}".format(target_layer))
        logging.info("target_table_name: {}".format(target_table_name))
        logging.info("mergeSchema: {}".format(mergeschema))
        logging.info(("process_type: {}".format(process_type)))

        logging.info("Checking whether the dataset to write is empty or not")
        if len(dataframe_to_write.head(1)) == 0:
            logging.info("No new partitions to write from source")
        elif (
            partitionby is None
            or partitionby == " "
            or partitionby == ""
            or mode is None
            or mode == ""
            or mode == " "
        ):
            raise ValueError(
                "Please check, partitionBy and Mode value can't be None or Empty for incremental load"
            )
        elif (
            read_layer is None
            or read_layer == ""
            or target_layer is None
            or target_layer == ""
        ):
            raise ValueError(
                "Please check, read_layer and target_layer value can't be None or Empty for incremental load"
            )
        else:
            if process_type.lower() == "latest":
                logging.info(
                    "Selecting only new data partition to write for latest process type scenario's"
                )
                target_max_data_load_date = self._get_metadata_max_data_date(
                    spark, target_table_name
                )
                tgt_filter_date_temp = target_max_data_load_date.rdd.flatMap(
                    lambda x: x
                ).collect()

                if (
                    tgt_filter_date_temp is None
                    or tgt_filter_date_temp == [None]
                    or tgt_filter_date_temp == ["None"]
                    or tgt_filter_date_temp == ""
                ):
                    raise ValueError(
                        "Please check the return date from _get_metadata_max_data_date function. It can't be empty"
                    )
                else:
                    tgt_filter_date = "".join(tgt_filter_date_temp)
                    logging.info(
                        "Max data date entry of lookup table in metadata table is: {}".format(
                            tgt_filter_date
                        )
                    )

                dataframe_to_write.createOrReplaceTempView("df_to_write")
                filter_col = partitionby

                df_with_latest_to_write = spark.sql(
                    "select * from df_to_write where {0} > to_date(cast('{1}' as String)) ".format(
                        filter_col, tgt_filter_date
                    )
                )
                if len(df_with_latest_to_write.head(1)) == 0:
                    logging.info(
                        "No new data to write for latest process type scenario's"
                    )
                else:
                    logging.info(
                        "Writing dataframe with latest process type scenario's"
                    )
                    rec_cnt = str(df_with_latest_to_write.count())
                    if str(df_with_latest_to_write.schema[partitionby].dataType) in (
                        ["TimestampType", "DateType"]
                    ):
                        df_with_latest_to_write = df_with_latest_to_write.withColumn(
                            partitionby,
                            F.col(partitionby).cast("date"),
                        )
                        df_with_latest_to_write.write.partitionBy(partitionby).mode(
                            mode
                        ).format(file_format).save(filewritepath)
                    else:
                        df_with_latest_to_write.write.partitionBy(partitionby).mode(
                            mode
                        ).format(file_format).save(filewritepath)

                    logging.info(
                        "Updating control table for latest process type scenario's"
                    )
                    self._update_metadata_table(
                        spark,
                        metadata_table_path,
                        target_table_name,
                        filewritepath,
                        mode,
                        process_type,
                        file_format,
                        partitionby,
                        read_layer,
                        target_layer,
                        rec_cnt,
                        mergeschema,
                    )

            elif process_type.lower() == "incremental":
                logging.info(
                    "Writing dataframe with incremental process type scenario's"
                )

                rec_cnt = str(dataframe_to_write.count())
                if str(dataframe_to_write.schema[partitionby].dataType) in (
                    ["TimestampType", "DateType"]
                ):
                    dataframe_to_write = dataframe_to_write.withColumn(
                        partitionby,
                        F.col(partitionby).cast("date"),
                    )
                    dataframe_to_write.write.partitionBy(partitionby).mode(mode).format(
                        file_format
                    ).save(filewritepath)
                else:
                    dataframe_to_write.write.partitionBy(partitionby).mode(mode).format(
                        file_format
                    ).save(filewritepath)

                logging.info(
                    "Updating control table for incremental process type scenario's "
                )

                self._update_metadata_table(
                    spark,
                    metadata_table_path,
                    target_table_name,
                    filewritepath,
                    mode,
                    process_type,
                    file_format,
                    partitionby,
                    read_layer,
                    target_layer,
                    rec_cnt,
                    mergeschema,
                )

            elif process_type.lower() == "full":
                logging.info(
                    "Checking if new partition is available for full load process type scenario's"
                )
                target_max_data_load_date = self._get_metadata_max_data_date(
                    spark, target_table_name
                )
                tgt_filter_date_temp = target_max_data_load_date.rdd.flatMap(
                    lambda x: x
                ).collect()

                if (
                    tgt_filter_date_temp is None
                    or tgt_filter_date_temp == [None]
                    or tgt_filter_date_temp == ["None"]
                    or tgt_filter_date_temp == ""
                ):
                    raise ValueError(
                        "Please check the return date from _get_metadata_max_data_date function. It can't be empty"
                    )
                else:
                    tgt_filter_date = "".join(tgt_filter_date_temp)
                    logging.info(
                        "Max data date entry of lookup table in metadata table is: {}".format(
                            tgt_filter_date
                        )
                    )

                dataframe_to_write.createOrReplaceTempView("df_to_write")
                filter_col = partitionby

                df_new_data_check = spark.sql(
                    "select * from df_to_write where {0} > to_date(cast('{1}' as String)) ".format(
                        filter_col, tgt_filter_date
                    )
                )

                if len(df_new_data_check.head(1)) == 0:
                    logging.info(
                        "No new data to write for full load process type scenario's"
                    )
                else:
                    logging.info(
                        "Writing dataframe with full load process type scenario's"
                    )

                    rec_cnt = str(dataframe_to_write.count())

                    if str(dataframe_to_write.schema[partitionby].dataType) in (
                        ["TimestampType", "DateType"]
                    ):
                        dataframe_to_write = dataframe_to_write.withColumn(
                            partitionby,
                            F.col(partitionby).cast("date"),
                        )
                        dataframe_to_write.write.mode(mode).format(file_format).save(
                            filewritepath
                        )
                    else:
                        dataframe_to_write.write.mode(mode).format(file_format).save(
                            filewritepath
                        )

                    logging.info(
                        "Updating control table for full load process type scenario's "
                    )
                    self._update_metadata_table(
                        spark,
                        metadata_table_path,
                        target_table_name,
                        filewritepath,
                        mode,
                        process_type,
                        file_format,
                        partitionby,
                        read_layer,
                        target_layer,
                        rec_cnt,
                        mergeschema,
                    )

            else:
                raise ValueError(
                    "Unknown process type. Please check. It can only be among 'incremental', 'latest', 'full "
                )

    def _load(self) -> DataFrame:
        """Helper function which gets called while reading a dataset via kedro"""
        logging.info("Entering load function")

        if self._process_type_load is not None and (
            self._process_type_load.lower() == "incremental"
            or self._process_type_load.lower() == "latest"
            or self._process_type_load.lower() == "full"
        ):
            logging.info(
                "Entering incremental load mode because process_type is 'incremental or latest or full'"
            )
            return self._get_incremental_data()

        else:
            print("load function")
            print(self._process_type_load)
            logging.info(
                "Skipping incremental load mode because process_type is not 'incremental or latest or full' "
            )
            load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))
            return self._get_spark().read.load(
                load_path, self._file_format, **self._load_args
            )

    def _save(self, data: DataFrame) -> None:
        """Helper function which gets called while saving a dataset via kedro"""
        logging.info("Entering save function")

        if self._process_type_save is not None and (
            self._process_type_save.lower() == "incremental"
            or self._process_type_save.lower() == "latest"
            or self._process_type_save.lower() == "full"
        ):
            logging.info(
                "Entering incremental save mode because process_type is 'incremental or latest or full'"
            )
            self._write_incremental_data(data)

        else:
            print(self._process_type_save)
            logging.info(
                "Skipping incremental save mode because process_type is not 'incremental or latest or full'"
            )
            save_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))
            data.write.save(save_path, self._file_format, **self._save_args)

    def _exists(self) -> bool:
        """Helper function to know whether a path exists or not"""
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        try:
            self._get_spark().read.load(load_path, self._file_format)
        except AnalysisException as exception:
            if exception.desc.startswith("Path does not exist:"):
                return False
            raise
        return True


class SparkDbfsDataSet(SparkDataSet):
    """
    Fixes bugs from SparkDataSet
    """

    def __init__(
        self,
        filepath: str,
        file_format: str = "parquet",
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        metadata_table_path: str = "",
        credentials: Dict[str, Any] = None,
    ) -> None:
        """Helper function to initialize SparkDbfsDatset"""
        super().__init__(
            filepath,
            file_format,
            load_args,
            save_args,
            version,
            metadata_table_path,
            credentials,
        )

        # # Fixes paths in Windows
        # if isinstance(self._filepath, WindowsPath):
        #     self._filepath = PurePosixPath(
        #         str(self._filepath).replace("\\", "/")
        #     )
