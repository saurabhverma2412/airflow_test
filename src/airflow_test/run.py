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
from pathlib import Path

from pathlib import Path
from typing import Iterable, Tuple

from kedro.framework.session import KedroSession
from kedro.framework.cli.utils import KedroCliError
from kedro.utils import load_obj
from itertools import chain
from datasets.spark_dbfs_dataset import SparkDbfsDataSet


def _get_values_as_tuple(values: Iterable[str]) -> Tuple[str, ...]:
    return tuple(chain.from_iterable(value.split(",") for value in values))


def run_package(pipeline: Iterable[str] = None, tag=None, env=None,
                parallel=None, runner=None, is_async=None, node_names=None,
                to_nodes=None, from_nodes=None, from_inputs=None,
                load_version=None, config=None, params=None,):
    # Entry point for running a Kedro project packaged with `kedro package`
    # using `python -m <project_package>.run` command.
    if parallel and runner:
        raise KedroCliError(
            "Both --parallel and --runner options cannot be used together. "
            "Please use either --parallel or --runner."
        )
    runner = runner or "SequentialRunner"
    if parallel:
        runner = "ParallelRunner"
    runner_class = load_obj(runner, "kedro.runner")

    package_name = Path(__file__).resolve().parent.name

    tag = _get_values_as_tuple(tag) if tag else tag
    node_names = _get_values_as_tuple(node_names) if node_names else node_names

    if (pipeline is not None) or (tag is not None) or (node_names is not None) \
            or (from_nodes is not None) or (to_nodes is not None) or (from_inputs is not None):
        for each_pipeline in pipeline:
            print(each_pipeline)
            with KedroSession.create(package_name, env=env, extra_params=params) as session:
                session.run(pipeline_name=each_pipeline,
                            tags=tag, runner=runner_class(is_async=is_async),
                            node_names=node_names, from_nodes=from_nodes, to_nodes=to_nodes,
                            from_inputs=from_inputs, load_versions=load_version)

    else:
        print("pass a pipeline/tags/from_nodes/to_nodes parameter to run")
        # with KedroSession.create(package_name) as session:
        #     session.run()


if __name__ == "__main__":
    run_package(pipeline=None)
