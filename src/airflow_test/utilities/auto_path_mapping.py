"""This module contains auto_path_mapping function which is used to get base project paths automatically
based on environment"""
import logging


def auto_path_mapping_project_context(catalog, running_environment):
    """
    Purpose: This function is used to automatically convert the source and target paths in catalog entries as per the
             working environment(dev/ prod).
    :param catalog:
    :param running_environment:
    :return:
    """
    temp_list = []
    if running_environment.lower() == "prod":
        metadata_table = catalog.load("params:env_path_conversion")["on_prod_metadata"]
        util_path = catalog.load("params:env_path_conversion")["on_prod_util"]
        dq_path = catalog.load("params:env_path_conversion")["on_prod_dq"]
        project_path = catalog.load("params:env_path_conversion")["project_path"]
    else:
        metadata_table = catalog.load("params:env_path_conversion")["on_dev_metadata"]
        util_path = catalog.load("params:env_path_conversion")["on_dev_util"]
        dq_path = catalog.load("params:env_path_conversion")["on_dev_dq"]
        project_path = catalog.load("params:env_path_conversion")["project_path"]

    for curr_catalog in catalog.list():
        if (
            type(catalog._data_sets[curr_catalog]).__name__ == "SparkDbfsDataSet"
            or type(catalog._data_sets[curr_catalog]).__name__
            == "SparkIgnoreMissingPathDataset"
        ):
            original_path = str(
                catalog._data_sets[curr_catalog].__getattribute__("_filepath")
            )
            original_path_lower = original_path.lower()

            if running_environment.lower() == "prod":
                if (
                    "staging/" in original_path_lower
                    or "intermediate/" in original_path_lower
                    or "primary/" in original_path_lower
                ):
                    new_target_path = original_path.replace(
                        "base_path", project_path + "/live"
                    )
                    catalog._data_sets[curr_catalog].__setattr__(
                        "_filepath", new_target_path
                    )
                    t_tuple = (original_path, new_target_path)
                    temp_list.append(t_tuple)
                else:
                    new_target_path = original_path.replace("base_path", project_path)
                    catalog._data_sets[curr_catalog].__setattr__(
                        "_filepath", new_target_path
                    )
                    t_tuple = (original_path, new_target_path)
                    temp_list.append(t_tuple)
            else:
                if (
                    "staging/" in original_path_lower
                    or "intermediate/" in original_path_lower
                    or "primary/" in original_path_lower
                ):
                    new_target_path = original_path.replace(
                        "base_path", project_path + "/dev"
                    )
                    catalog._data_sets[curr_catalog].__setattr__(
                        "_filepath", new_target_path
                    )
                    t_tuple = (original_path, new_target_path)
                    temp_list.append(t_tuple)

                else:
                    new_target_path = original_path.replace("base_path", project_path)
                    catalog._data_sets[curr_catalog].__setattr__(
                        "_filepath", new_target_path
                    )
                    t_tuple = (original_path, new_target_path)
                    temp_list.append(t_tuple)

            try:
                meta_data_path = str(
                    catalog._data_sets[curr_catalog].__getattribute__(
                        "_metadata_table_path"
                    )
                )
                new_meta_data_path = meta_data_path.replace(
                    "metadata_path", metadata_table
                )
                catalog._data_sets[curr_catalog].__setattr__(
                    "_metadata_table_path", new_meta_data_path
                )
            except Exception:  # pylint: disable=broad-except
                logging.info("No Meta-Data Found While Replacing Paths")

            if "/utilities/" in original_path_lower:
                new_util_path = original_path.replace("util_path", util_path)
                catalog._data_sets[curr_catalog].__setattr__("_filepath", new_util_path)
                t_tuple = (original_path, new_util_path)
                temp_list.append(t_tuple)

            if "/dq/" in original_path_lower:
                new_dq_path = original_path.replace("dq_path", dq_path)
                catalog._data_sets[curr_catalog].__setattr__("_filepath", new_dq_path)
                t_tuple = (original_path, new_dq_path)
                temp_list.append(t_tuple)

        elif type(catalog._data_sets[curr_catalog]).__name__ == "SparkDataSet":
            original_path = str(
                catalog._data_sets[curr_catalog].__getattribute__("_filepath")
            )
            original_path_lower = original_path.lower()
            if "/dq/" in original_path_lower:
                new_dq_path = original_path.replace("dq_path", dq_path)
                catalog._data_sets[curr_catalog].__setattr__("_filepath", new_dq_path)
                t_tuple = (original_path, new_dq_path)
                temp_list.append(t_tuple)

    return catalog
