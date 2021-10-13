# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Entry point for running a Kedro pipeline as a Python package."""
import datetime
import logging
import logging.config
import os
from pathlib import Path
from typing import Any, Dict, Union

from kedro.framework.context import KedroContext
from kedro.framework.hooks import get_hook_manager
from kedro.io import DataCatalog
from kedro.versioning import Journal
from pyspark import SparkConf
from pyspark.sql import SparkSession

# from spaceflights_17_4.utilities.auto_path_mapping import (
#     auto_path_mapping_project_context,
# )

# conf = os.getenv("CONF", "base")
# running_environment = os.getenv("RUNNING_ENVIRONMENT", "dev")

LOG_FILE_NAME = str(datetime.datetime.now().strftime("%Y_%m_%d_%H_%M"))

# print("conf: ", conf)
# print("running_environment: ", running_environment)


class ProjectContext(KedroContext):
    """A subclass of KedroContext to add Spark initialisation for the pipeline.

    Users can override the remaining methods from the parent class here,
    or create new ones (e.g. as required by plugins)

    """

    def __init__(
        self,
        package_name: str,
        project_path: Union[Path, str],
        env: str = None,
        extra_params: Dict[str, Any] = None,
    ):
        super().__init__(package_name, project_path, env, extra_params)
        self.init_spark_session()

    def init_spark_session(self) -> None:
        """Initialises a SparkSession using the config defined in project's conf folder."""

        # Load the spark configuration in spark.yaml using the config loader
        parameters = self.config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(self.package_name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")

    def _setup_logging(self) -> None:
        """Register logging specified in logging directory."""

        conf_logging = self.config_loader.get("logging*", "logging*/**")
        info_file_path = conf_logging["handlers"]["info_file_handler"]["filename"]
        info_file_path_new = info_file_path.replace(".", "_{}.".format(LOG_FILE_NAME))

        error_file_path = conf_logging["handlers"]["error_file_handler"]["filename"]
        error_file_path_new = error_file_path.replace(".", "_{}.".format(LOG_FILE_NAME))

        conf_logging["handlers"]["info_file_handler"]["filename"] = info_file_path_new
        conf_logging["handlers"]["error_file_handler"]["filename"] = error_file_path_new
        logging.config.dictConfig(conf_logging)

    def _get_catalog(
        self,
        save_version: str = None,
        journal: Journal = None,
        load_versions: Dict[str, str] = None,
    ) -> DataCatalog:
        """A hook for changing the creation of a DataCatalog instance.

        Returns:
            DataCatalog defined in `catalog.yml`.
        Raises:
            KedroContextError: Incorrect ``DataCatalog`` registered for the project.

        """
        # '**/catalog*' reads modular pipeline configs
        conf_catalog = self.config_loader.get("catalog*", "catalog*/**", "**/catalog*")
        conf_creds = self._get_config_credentials()

        hook_manager = get_hook_manager()
        catalog = hook_manager.hook.register_catalog(  # pylint: disable=no-member
            catalog=conf_catalog,
            credentials=conf_creds,
            load_versions=load_versions,
            save_version=save_version,
            journal=journal,
        )
        if not isinstance(catalog, DataCatalog):
            raise self.KedroContextError(
                f"Expected an instance of `DataCatalog`, "
                f"got `{type(catalog).__name__}` instead."
            )

        feed_dict = self._get_feed_dict()
        catalog.add_feed_dict(feed_dict)
        # if catalog.layers:
        #     self._validate_layers_for_transcoding(catalog)
        hook_manager = get_hook_manager()
        hook_manager.hook.after_catalog_created(  # pylint: disable=no-member
            catalog=catalog,
            conf_catalog=conf_catalog,
            conf_creds=conf_creds,
            feed_dict=feed_dict,
            save_version=save_version,
            load_versions=load_versions,
            run_id=self.run_id,
        )

        #catalog = auto_path_mapping_project_context(catalog, running_environment)
        return catalog
