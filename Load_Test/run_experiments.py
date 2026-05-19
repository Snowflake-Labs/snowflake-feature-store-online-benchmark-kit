#!/usr/bin/env python3
"""
Unified Config-Based Experiment Runner for Feature Store Load Tests.

Supports both Snowflake Feature Store Postgres and Databricks Online Feature Store
via the --platform flag. Runs experiments from JSON configuration files with
parameter inheritance and series support.

Usage:
    python run_experiments.py --platform snowflake --config config.json [options]
    python run_experiments.py --platform databricks --config config.json [options]
"""

import argparse
import subprocess
import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Any

from experiment_config import load_config, expand_experiments
from dotenv import load_dotenv
import importlib


def create_results_directory(output_dir: str = "results") -> tuple:
    """Create timestamped results directory."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    results_dir = f"{output_dir}/{timestamp}"
    os.makedirs(results_dir, exist_ok=True)
    return results_dir, timestamp


def set_experiment_env_vars(experiment: Dict[str, Any], platform: str) -> None:
    """Set experiment parameters as environment variables for Locust workers."""
    params = experiment["params"]

    for param_name, param_value in params.items():
        env_var_name = f"EXPERIMENT_PARAM_{param_name.upper()}"
        os.environ[env_var_name] = str(param_value)

    if "series_class" in experiment:
        os.environ["EXPERIMENT_PARAM_SERIES_CLASS"] = experiment["series_class"]

    if "series_name" in experiment:
        os.environ["EXPERIMENT_PARAM_SERIES_NAME"] = experiment["series_name"]

    os.environ["EXPERIMENT_PARAM_PLATFORM"] = platform


def load_series_class(series_class_name: str, platform: str):
    """
    Dynamically import and instantiate a series class from the
    appropriate platform-specific module.
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)

        module_name = f"series_{platform}"
        series_module = importlib.import_module(module_name)
        series_class = getattr(series_module, series_class_name)
        return series_class()
    except (ImportError, AttributeError) as e:
        raise ValueError(
            f"Failed to load series class '{series_class_name}' from series_{platform}: {e}"
        )


def create_snowflake_session():
    """Create a Snowflake Snowpark session from environment variables."""
    from snowflake.snowpark.session import Session

    load_dotenv(override=True)

    connection_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "host": os.getenv("SNOWFLAKE_HOST"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }

    missing_params = [k for k, v in connection_params.items() if not v]
    if missing_params:
        raise ValueError(f"Missing Snowflake parameters: {missing_params}")

    pat = os.getenv("SNOWFLAKE_PAT")
    if not pat:
        raise ValueError("SNOWFLAKE_PAT environment variable is required")

    return Session.builder.configs(connection_params).create()


def create_databricks_config() -> Dict[str, str]:
    """Build a client_config dict from Databricks environment variables."""
    load_dotenv(override=True)

    config = {
        "host": os.getenv("DATABRICKS_HOST", ""),
        "token": os.getenv("DATABRICKS_TOKEN", ""),
        "catalog": os.getenv("DATABRICKS_CATALOG", ""),
        "schema": os.getenv("DATABRICKS_SCHEMA", ""),
        "online_store_name": os.getenv("DATABRICKS_ONLINE_STORE_NAME", ""),
        "endpoint_name": os.getenv("DATABRICKS_SERVING_ENDPOINT", ""),
    }

    missing = [k for k, v in config.items() if not v and k not in ("online_store_name", "endpoint_name")]
    if missing:
        raise ValueError(f"Missing Databricks configuration: {missing}")

    return config


def run_locust_experiment(
    experiment: Dict[str, Any], results_dir: str, skip_warmup: bool = False
) -> Dict[str, Any]:
    """Run a single Locust experiment and parse results."""
    series_name = experiment["series_name"]
    experiment_id = experiment["experiment_id"]
    params = experiment["params"]
    system_config = experiment["system"]

    series_dir = os.path.join(results_dir, series_name)
    os.makedirs(series_dir, exist_ok=True)

    exp_dir = os.path.join(series_dir, experiment_id)
    os.makedirs(exp_dir, exist_ok=True)

    qps = params["qps"]
    duration = system_config.get("duration", 180)
    warmup = system_config.get("warmup", 180)
    if skip_warmup:
        warmup = min(60, warmup)

    print(f"Running experiment {experiment_id}: {params}")

    users_multiplier = system_config.get("users_multiplier", 1)
    users = qps // users_multiplier
    if users < 5:
        users = 5
        users_multiplier = qps / users

    params["users_multiplier"] = users_multiplier

    num_cores = int(os.getenv("NUM_CORES", os.cpu_count() or 1))
    processes = min(num_cores, users)

    total_run_time = duration + warmup

    script_dir = os.path.dirname(os.path.abspath(__file__))
    locustfile_path = os.path.join(script_dir, "locustfile.py")

    set_experiment_env_vars(experiment, experiment.get("platform", "snowflake"))

    cmd = [
        "locust",
        "--headless",
        f"--users={users}",
        f"--spawn-rate={users / warmup}",
        f"--run-time={total_run_time}s",
        f"--html={exp_dir}/results.html",
        f"--csv={exp_dir}/results",
        f"--locustfile={locustfile_path}",
        "--reset-stats",
    ]

    if processes > 1:
        cmd.insert(1, f"--processes={processes}")

    try:
        result = subprocess.run(cmd, text=False, timeout=total_run_time + 120)

        if result.returncode != 0:
            print(
                f"Locust experiment failed for {experiment_id} with return code {result.returncode}"
            )
            return None

        import pandas as pd

        stats_file = f"{exp_dir}/results_stats.csv"
        df_stats = pd.read_csv(stats_file)

        aggregate_row = df_stats.iloc[-1]

        total_requests = int(aggregate_row.get("Request Count", 0))

        print(
            f"  Post-warmup stats: Total requests={total_requests}, "
            f"P50={aggregate_row.get('50%', 0):.1f}ms, "
            f"P90={aggregate_row.get('90%', 0):.1f}ms, "
            f"P95={aggregate_row.get('95%', 0):.1f}ms"
        )

        experiment_results = {
            "experiment_id": experiment_id,
            "series_name": series_name,
            "platform": experiment.get("platform", "snowflake"),
            "params": params,
            "system": system_config,
            "x_axis_parameter": experiment.get("x_axis_parameter"),
            "total_requests": total_requests,
            "success_rate": 100.0
            - float(aggregate_row.get("Failure Rate", 0)) * 100,
            "avg_response_time": float(
                aggregate_row.get("Average Response Time", 0)
            ),
            "min_response_time": float(
                aggregate_row.get("Min Response Time", 0)
            ),
            "max_response_time": float(
                aggregate_row.get("Max Response Time", 0)
            ),
            "p50_response_time": float(aggregate_row.get("50%", 0)),
            "p90_response_time": float(aggregate_row.get("90%", 0)),
            "p95_response_time": float(aggregate_row.get("95%", 0)),
            "p99_response_time": float(aggregate_row.get("99%", 0)),
            "p999_response_time": float(aggregate_row.get("99.9%", 0)),
            "requests_per_second": float(total_requests / duration)
            if duration > 0
            else 0,
            "timestamp": experiment["timestamp"],
        }

        with open(f"{exp_dir}/results.json", "w") as f:
            json.dump(experiment_results, f, indent=2)

        with open(f"{exp_dir}/params.json", "w") as f:
            json.dump(experiment, f, indent=2)

        return experiment_results

    except subprocess.TimeoutExpired:
        print(f"Locust experiment timed out for {experiment_id}")
        return None
    except Exception as e:
        print(f"Error running Locust experiment for {experiment_id}: {e}")
        return None


def _filter_series_by_query_mode(series_groups: Dict[str, List], query_mode: str, platform: str) -> Dict[str, List]:
    """
    Filter series groups to only include series matching the requested query mode.
    For Databricks, checks whether the series inherits from BaseSqlSeries.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

    series_module = importlib.import_module(f"series_{platform}")

    filtered = {}
    for series_name, experiments in series_groups.items():
        series_class_name = experiments[0]["series_class"]
        series_class = getattr(series_module, series_class_name)

        if hasattr(series_module, "BaseSqlSeries"):
            is_sql = issubclass(series_class, series_module.BaseSqlSeries)
        else:
            is_sql = False

        mode_lower = query_mode.lower()
        if (mode_lower == "sql" and is_sql) or (mode_lower == "rest" and not is_sql):
            filtered[series_name] = experiments
        else:
            print(f"Skipping series '{series_name}' ({series_class_name}) — query_mode={query_mode}")

    return filtered


def run_experiments_from_config(
    config_file: str,
    platform: str,
    series_filter: str = None,
    dry_run: bool = False,
    query_mode: str = "REST",
) -> None:
    """Run experiments from configuration file."""
    print(f"Loading configuration from: {config_file}")
    print(f"Platform: {platform}")
    print(f"Query mode: {query_mode}")
    config = load_config(config_file)

    series_groups = expand_experiments(config)

    if series_filter:
        if series_filter not in series_groups:
            print(f"No experiments found for series: {series_filter}")
            return
        series_groups = {series_filter: series_groups[series_filter]}

    if platform == "databricks":
        series_groups = _filter_series_by_query_mode(series_groups, query_mode, platform)

    total_experiments = sum(len(exps) for exps in series_groups.values())
    print(f"Found {total_experiments} experiments to run")

    if dry_run:
        print("\nDry run - experiments that would be executed:")
        for series_name, experiments in series_groups.items():
            for exp in experiments:
                params_str = ", ".join(
                    f"{k}={v}" for k, v in exp["params"].items()
                )
                print(f"  {exp['experiment_id']}: {params_str}")
        return

    output_dir = config.get("system", {}).get("output_dir", "results")
    results_dir, timestamp = create_results_directory(output_dir)
    print(f"Results will be saved to: {results_dir}")

    with open(f"{results_dir}/experiment_config.json", "w") as f:
        json.dump(config, f, indent=2)

    successful_experiments = []
    failed_experiments = []

    for series_name, series_experiments in series_groups.items():
        print(f"\n{'=' * 60}")
        print(f"Starting series: {series_name}")
        print(f"{'=' * 60}")

        series_class_name = series_experiments[0]["series_class"]
        series_instance = load_series_class(series_class_name, platform)

        session_or_config = None
        try:
            if platform == "snowflake":
                session_or_config = create_snowflake_session()
            else:
                session_or_config = create_databricks_config()

            series_params = series_experiments[0]["params"].copy()
            series_params["series_name"] = series_name
            series_params["query_mode"] = query_mode
            os.environ["EXPERIMENT_PARAM_QUERY_MODE"] = query_mode

            series_instance.setup_series(session_or_config, series_params)
            print(f"Loaded series class {series_class_name}")

            for i, experiment in enumerate(series_experiments):
                print(f"\n{'=' * 60}")
                print(
                    f"Running experiment {i + 1}/{len(series_experiments)} "
                    f"in series '{series_name}': {experiment['experiment_id']}"
                )
                print(f"{'=' * 60}")

                experiment["platform"] = platform

                skip_warmup = series_instance.can_skip_re_warmup and i > 0

                series_instance.setup_experiment(
                    session_or_config, experiment["params"]
                )

                result = run_locust_experiment(
                    experiment, results_dir, skip_warmup
                )

                series_instance.teardown_experiment(
                    session_or_config, experiment["params"]
                )

                if result:
                    successful_experiments.append(result)
                    print(f"Experiment completed successfully")
                    print(f"  Success Rate: {result['success_rate']:.2f}%")
                    print(
                        f"  Avg Response Time: {result['avg_response_time']:.2f}ms"
                    )
                    print(
                        f"  P95 Response Time: {result['p95_response_time']:.2f}ms"
                    )
                else:
                    failed_experiments.append(experiment)
                    print(f"Experiment failed")

                if i < len(series_experiments) - 1:
                    print("Waiting 3 seconds before next experiment...")
                    time.sleep(3)

            series_instance.teardown_series(session_or_config, series_params)

        except Exception as e:
            print(f"\nERROR in series '{series_name}': {e}")
            import traceback
            traceback.print_exc()

            for exp in series_experiments:
                failed_experiments.append(exp)

            if session_or_config:
                try:
                    series_instance.teardown_series(session_or_config, series_params)
                except Exception:
                    pass

        finally:
            if platform == "snowflake" and session_or_config:
                try:
                    session_or_config.close()
                except Exception:
                    pass

    total_experiments_count = sum(len(exps) for exps in series_groups.values())
    run_summary = {
        "timestamp": timestamp,
        "config_file": config_file,
        "platform": platform,
        "total_experiments": total_experiments_count,
        "successful_experiments": len(successful_experiments),
        "failed_experiments": len(failed_experiments),
        "successful": [exp["experiment_id"] for exp in successful_experiments],
        "failed": [exp["experiment_id"] for exp in failed_experiments],
        "results": successful_experiments,
    }

    with open(f"{results_dir}/run_summary.json", "w") as f:
        json.dump(run_summary, f, indent=2)

    print(f"\n{'=' * 60}")
    print("EXPERIMENT RUN COMPLETE")
    print(f"{'=' * 60}")
    print(f"Platform: {platform}")
    print(f"Total experiments: {total_experiments_count}")
    print(f"Successful: {len(successful_experiments)}")
    print(f"Failed: {len(failed_experiments)}")
    print(f"Results directory: {results_dir}")

    if failed_experiments:
        print(f"\nFailed experiments:")
        for exp in failed_experiments:
            print(f"  - {exp['experiment_id']}")

    print(f"\nNext steps:")
    print(
        f"  1. Generate report: python main-load-test-combined/generate_report.py {results_dir}"
    )
    print(
        f"  2. View results: open {results_dir}/final_report.html"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Unified Feature Store load test experiment runner (Snowflake + Databricks)"
    )
    parser.add_argument(
        "--platform",
        required=True,
        choices=["snowflake", "databricks"],
        help="Target platform: 'snowflake' or 'databricks'",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to experiment configuration JSON file",
    )
    parser.add_argument(
        "--series",
        help="Run only a specific series by name (optional)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running",
    )
    parser.add_argument(
        "--query-mode",
        choices=["REST", "SQL"],
        default="REST",
        help="Query mode: REST (default, HTTP API) or SQL (direct SQL queries against warehouse/DT)",
    )

    args = parser.parse_args()

    try:
        run_experiments_from_config(
            config_file=args.config,
            platform=args.platform,
            series_filter=args.series,
            dry_run=args.dry_run,
            query_mode=args.query_mode,
        )
    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
