#!/usr/bin/env python3
"""
Unified Experiment Configuration Loader for Feature Store Load Tests.

Handles loading, validating, and processing experiment configurations
from JSON files with parameter inheritance support.

Supports parameters for both Snowflake and Databricks platforms.
"""

import json
import os
from typing import Dict, List, Tuple, Any
from datetime import datetime


VALID_TASK_TYPES = {"query", "ingest", "mixed"}
VALID_QUERY_MODES = {"REST", "SQL"}

VALID_ONLINE_STORE_CAPACITIES = {"CU_1", "CU_2", "CU_4", "CU_8"}
VALID_ENDPOINT_WORKLOAD_SIZES = {"Small", "Medium", "Large"}
VALID_SYNC_MODES = {"SNAPSHOT", "TRIGGERED", "CONTINUOUS"}

VALID_EXPERIMENT_PARAMS = {
    # Shared parameters
    "qps",
    "batch_size",
    "num_columns",
    "num_table_rows",
    "nop",
    # Snowflake-specific
    "num_entity_keys",
    "task_type",
    "ingest_keys_per_minute",
    # Databricks-specific
    "online_store_capacity",
    "endpoint_workload_size",
    "sync_mode",
}

VALID_X_AXIS_PARAMS = {
    "qps",
    "batch_size",
    "num_columns",
    "num_table_rows",
    "num_entity_keys",
    "nop",
    "ingest_keys_per_minute",
    "online_store_capacity",
    "endpoint_workload_size",
    "sync_mode",
    "duration",
    "warmup",
}


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load and validate configuration from JSON file.

    Args:
        config_file: Path to JSON configuration file

    Returns:
        dict: Loaded configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If JSON is invalid
        ValueError: If configuration is invalid
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    with open(config_file, "r") as f:
        config = json.load(f)

    is_valid, errors = validate_config(config)
    if not is_valid:
        error_msg = "Configuration validation failed:\n" + "\n".join(
            f"  - {error}" for error in errors
        )
        raise ValueError(error_msg)

    return config


def merge_params(golden: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge override parameters with golden/base parameters."""
    result = golden.copy()
    result.update(override)
    return result


def expand_experiments(config: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Expand series into individual experiment configurations, grouped by series.

    Args:
        config: Full configuration dictionary

    Returns:
        dict: Dictionary mapping series_name to list of experiment configurations
    """
    series_groups = {}
    base_experiment_settings = config.get("base_experiment_settings", {})
    system_config = config.get("system", {})
    series_list = config.get("series", [])

    for series in series_list:
        series_name = series["name"]
        series_experiments = series.get("experiments", [])
        experiments = []

        for i, experiment_params in enumerate(series_experiments):
            merged_params = merge_params(base_experiment_settings, experiment_params)
            experiment_id = f"{series_name}_{i + 1:03d}"

            experiment_config = {
                "series_name": series_name,
                "experiment_id": experiment_id,
                "params": merged_params,
                "system": system_config,
                "series_class": series.get("series_class"),
                "x_axis_parameter": series.get("x_axis_parameter", "qps"),
                "timestamp": datetime.now().isoformat(),
            }

            experiments.append(experiment_config)

        series_groups[series_name] = experiments

    return series_groups


def validate_config(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate configuration structure and values.

    Accepts the union of valid parameters for both Snowflake and Databricks.

    Args:
        config: Configuration dictionary to validate

    Returns:
        tuple: (is_valid, list_of_errors)
    """
    errors = []

    required_keys = ["system", "base_experiment_settings", "series"]
    for key in required_keys:
        if key not in config:
            errors.append(f"Missing required key: {key}")

    if errors:
        return False, errors

    # Validate system configuration
    system = config["system"]
    if not isinstance(system, dict):
        errors.append("'system' must be a dictionary")
    else:
        if "duration" not in system:
            errors.append("'system.duration' is required")
        elif not isinstance(system["duration"], int) or system["duration"] < 1:
            errors.append("'system.duration' must be a positive integer")

        if "warmup" not in system:
            errors.append("'system.warmup' is required")
        elif not isinstance(system["warmup"], int) or system["warmup"] < 0:
            errors.append("'system.warmup' must be a non-negative integer")

        if "users_multiplier" not in system:
            errors.append("'system.users_multiplier' is required")
        elif not isinstance(system["users_multiplier"], int) or system["users_multiplier"] < 1:
            errors.append("'system.users_multiplier' must be a positive integer")

    # Validate base_experiment_settings
    base = config["base_experiment_settings"]
    if not isinstance(base, dict):
        errors.append("'base_experiment_settings' must be a dictionary")
    else:
        required_base = ["qps", "batch_size", "num_columns", "num_table_rows"]
        for param in required_base:
            if param not in base:
                errors.append(f"'base_experiment_settings.{param}' is required")
            else:
                value = base[param]
                if not isinstance(value, int) or value < 1:
                    errors.append(
                        f"'base_experiment_settings.{param}' must be an integer >= 1"
                    )

        if "num_entity_keys" in base:
            if not isinstance(base["num_entity_keys"], int) or base["num_entity_keys"] < 1:
                errors.append("'base_experiment_settings.num_entity_keys' must be an integer >= 1")

        if "task_type" in base:
            if base["task_type"] not in VALID_TASK_TYPES:
                errors.append(
                    f"'base_experiment_settings.task_type' must be one of: {', '.join(sorted(VALID_TASK_TYPES))}"
                )

        if "online_store_capacity" in base:
            if base["online_store_capacity"] not in VALID_ONLINE_STORE_CAPACITIES:
                errors.append(
                    f"'base_experiment_settings.online_store_capacity' must be one of: {VALID_ONLINE_STORE_CAPACITIES}"
                )

        if "endpoint_workload_size" in base:
            if base["endpoint_workload_size"] not in VALID_ENDPOINT_WORKLOAD_SIZES:
                errors.append(
                    f"'base_experiment_settings.endpoint_workload_size' must be one of: {VALID_ENDPOINT_WORKLOAD_SIZES}"
                )

        if "sync_mode" in base:
            if base["sync_mode"] not in VALID_SYNC_MODES:
                errors.append(
                    f"'base_experiment_settings.sync_mode' must be one of: {VALID_SYNC_MODES}"
                )

        if "nop" in base:
            if not isinstance(base["nop"], int) or base["nop"] < 1:
                errors.append("'base_experiment_settings.nop' must be an integer >= 1")

    # Validate series
    series_list = config["series"]
    if not isinstance(series_list, list):
        errors.append("'series' must be a list")
    elif len(series_list) == 0:
        errors.append("'series' must contain at least one series")
    else:
        series_names = set()
        for i, series in enumerate(series_list):
            if not isinstance(series, dict):
                errors.append(f"Series {i} must be a dictionary")
                continue

            if "name" not in series:
                errors.append(f"Series {i} missing 'name'")
                series_name = f"<unnamed_{i}>"
            else:
                series_name = series["name"]
                if not isinstance(series_name, str):
                    errors.append(f"Series {i} 'name' must be a string")
                elif not series_name.replace("_", "").replace("-", "").isalnum():
                    errors.append(
                        f"Series {i} 'name' must contain only alphanumeric characters, underscores, and hyphens"
                    )
                elif series_name in series_names:
                    errors.append(f"Duplicate series name: {series_name}")
                else:
                    series_names.add(series_name)

            if "series_class" not in series:
                errors.append(f"Series '{series_name}' missing 'series_class'")
            elif (
                not isinstance(series["series_class"], str)
                or len(series["series_class"]) == 0
            ):
                errors.append(
                    f"Series '{series_name}' 'series_class' must be a non-empty string"
                )

            if "x_axis_parameter" in series:
                x_axis_param = series["x_axis_parameter"]
                if not isinstance(x_axis_param, str):
                    errors.append(
                        f"Series '{series_name}' 'x_axis_parameter' must be a string"
                    )
                elif x_axis_param not in VALID_X_AXIS_PARAMS:
                    errors.append(
                        f"Series '{series_name}' 'x_axis_parameter' must be one of: {', '.join(sorted(VALID_X_AXIS_PARAMS))}"
                    )

            if "experiments" not in series:
                errors.append(f"Series '{series_name}' missing 'experiments'")
            else:
                experiments = series["experiments"]
                if not isinstance(experiments, list):
                    errors.append(
                        f"Series '{series_name}' 'experiments' must be a list"
                    )
                elif len(experiments) == 0:
                    errors.append(
                        f"Series '{series_name}' must contain at least one experiment"
                    )
                else:
                    for j, experiment in enumerate(experiments):
                        if not isinstance(experiment, dict):
                            errors.append(
                                f"Series '{series_name}' experiment {j} must be a dictionary"
                            )
                        else:
                            for param_name, param_value in experiment.items():
                                if param_name == "task_type":
                                    if param_value not in VALID_TASK_TYPES:
                                        errors.append(
                                            f"Series '{series_name}' experiment {j} 'task_type' must be one of: {', '.join(sorted(VALID_TASK_TYPES))}"
                                        )
                                elif param_name == "online_store_capacity":
                                    if param_value not in VALID_ONLINE_STORE_CAPACITIES:
                                        errors.append(
                                            f"Series '{series_name}' experiment {j} 'online_store_capacity' must be one of: {VALID_ONLINE_STORE_CAPACITIES}"
                                        )
                                elif param_name == "endpoint_workload_size":
                                    if param_value not in VALID_ENDPOINT_WORKLOAD_SIZES:
                                        errors.append(
                                            f"Series '{series_name}' experiment {j} 'endpoint_workload_size' must be one of: {VALID_ENDPOINT_WORKLOAD_SIZES}"
                                        )
                                elif param_name == "sync_mode":
                                    if param_value not in VALID_SYNC_MODES:
                                        errors.append(
                                            f"Series '{series_name}' experiment {j} 'sync_mode' must be one of: {VALID_SYNC_MODES}"
                                        )
                                elif param_name == "ingest_keys_per_minute":
                                    if not isinstance(param_value, int) or param_value < 0:
                                        errors.append(
                                            f"Series '{series_name}' experiment {j} 'ingest_keys_per_minute' must be an integer >= 0"
                                        )
                                elif param_name in VALID_EXPERIMENT_PARAMS:
                                    if isinstance(param_value, int) and param_value < 1:
                                        errors.append(
                                            f"Series '{series_name}' experiment {j} '{param_name}' must be an integer >= 1"
                                        )

    return len(errors) == 0, errors


def get_experiment_summary(
    series_groups: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, Any]:
    """Generate a summary of experiments for reporting."""
    all_experiments = []
    for experiments in series_groups.values():
        all_experiments.extend(experiments)

    series_count = len(series_groups)
    total_experiments = len(all_experiments)
    series_breakdown = {name: len(exps) for name, exps in series_groups.items()}

    return {
        "total_series": series_count,
        "total_experiments": total_experiments,
        "series_breakdown": series_breakdown,
        "estimated_runtime_minutes": sum(
            exp["system"]["duration"] for exp in all_experiments
        )
        // 60,
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python experiment_config.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    try:
        config = load_config(config_file)
        experiments = expand_experiments(config)
        summary = get_experiment_summary(experiments)

        print(f"Configuration loaded successfully")
        print(
            f"{summary['total_series']} series, {summary['total_experiments']} experiments"
        )
        print(f"Estimated runtime: {summary['estimated_runtime_minutes']} minutes")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
