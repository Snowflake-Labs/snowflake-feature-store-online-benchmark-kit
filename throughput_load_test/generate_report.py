#!/usr/bin/env python3
"""
Unified Report Generator for Feature Store Load Test Results.

Supports both Snowflake and Databricks platforms, adapting branding
and table columns based on the platform recorded in results metadata.
"""

import argparse
import json
import os
import base64
import io
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd


def load_test_results(results_dir):
    """Load all test results from the results directory."""
    results = []

    series_dirs = [
        d
        for d in os.listdir(results_dir)
        if os.path.isdir(os.path.join(results_dir, d)) and d != "."
    ]

    for series_dir in series_dirs:
        series_path = os.path.join(results_dir, series_dir)

        experiment_dirs = [
            d
            for d in os.listdir(series_path)
            if os.path.isdir(os.path.join(series_path, d))
        ]

        for exp_dir in experiment_dirs:
            exp_path = os.path.join(series_path, exp_dir)
            json_file = os.path.join(exp_path, "results.json")
            params_file = os.path.join(exp_path, "params.json")

            if os.path.exists(json_file):
                try:
                    with open(json_file, "r") as f:
                        result = json.load(f)

                    if "x_axis_parameter" not in result and os.path.exists(
                        params_file
                    ):
                        try:
                            with open(params_file, "r") as pf:
                                params_data = json.load(pf)
                                result["x_axis_parameter"] = params_data.get(
                                    "x_axis_parameter", "qps"
                                )
                        except Exception:
                            result["x_axis_parameter"] = "qps"
                    elif "x_axis_parameter" not in result:
                        result["x_axis_parameter"] = "qps"

                    results.append(result)
                except Exception as e:
                    print(f"Error loading {json_file}: {e}")

    results.sort(
        key=lambda x: (x.get("series_name", ""), x.get("experiment_id", ""))
    )
    return results


def detect_platform(results, results_dir):
    """Detect platform from results metadata or run_summary."""
    for r in results:
        if "platform" in r:
            return r["platform"]

    summary_file = os.path.join(results_dir, "run_summary.json")
    if os.path.exists(summary_file):
        try:
            with open(summary_file, "r") as f:
                summary = json.load(f)
                return summary.get("platform", "snowflake")
        except Exception:
            pass

    return "snowflake"


def create_latency_chart_for_series(series_results):
    """Create latency chart for a single series."""
    if not series_results:
        return None

    x_axis_param = series_results[0].get("x_axis_parameter", "qps")

    series_results.sort(
        key=lambda x: (
            x.get("params", {}).get(x_axis_param, 0)
            if isinstance(x.get("params", {}).get(x_axis_param, 0), (int, float))
            else 0
        )
    )

    x_values = [r.get("params", {}).get(x_axis_param, 0) for r in series_results]

    if x_values and isinstance(x_values[0], str):
        x_labels = x_values
        x_values = list(range(len(x_labels)))
        use_labels = True
    else:
        use_labels = False

    percentiles = ["p50_response_time", "p90_response_time", "p95_response_time"]
    percentile_labels = ["P50", "P90", "P95"]
    colors = ["blue", "green", "orange"]

    fig, ax = plt.subplots(figsize=(10, 6))

    for percentile, label, color in zip(percentiles, percentile_labels, colors):
        values = [r.get(percentile, 0) for r in series_results]
        ax.plot(
            x_values,
            values,
            "o-",
            color=color,
            linewidth=2,
            markersize=6,
            label=label,
        )

    ax.yaxis.set_major_locator(ticker.MultipleLocator(10))

    series_name = series_results[0].get("series_name", "unknown")
    task_type = series_results[0].get("params", {}).get("task_type", "query")
    ax.set_title(
        f"{series_name} ({task_type}) - Response Time Percentiles",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_ylabel("Response Time (ms)", fontsize=12)
    ax.set_xlabel(x_axis_param.replace("_", " ").title(), fontsize=12)

    if use_labels:
        ax.set_xticks(x_values)
        ax.set_xticklabels(x_labels, rotation=45, ha="right")

    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format="png", dpi=300, bbox_inches="tight")
    img_buffer.seek(0)
    img_base64 = base64.b64encode(img_buffer.getvalue()).decode()
    plt.close()

    return img_base64


def create_success_rate_chart_for_series(series_results):
    """Create success rate chart for a single series."""
    if not series_results:
        return None

    x_axis_param = series_results[0].get("x_axis_parameter", "qps")

    series_results.sort(
        key=lambda x: (
            x.get("params", {}).get(x_axis_param, 0)
            if isinstance(x.get("params", {}).get(x_axis_param, 0), (int, float))
            else 0
        )
    )

    x_values = [r.get("params", {}).get(x_axis_param, 0) for r in series_results]
    success_rates = [r.get("success_rate", 0) for r in series_results]

    if x_values and isinstance(x_values[0], str):
        x_labels = x_values
        x_values = list(range(len(x_labels)))
        use_labels = True
    else:
        use_labels = False

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(x_values, success_rates, "bo-", linewidth=2, markersize=8)
    series_name = series_results[0].get("series_name", "unknown")
    ax.set_title(
        f"{series_name} - Success Rate", fontsize=14, fontweight="bold"
    )
    ax.set_xlabel(x_axis_param.replace("_", " ").title(), fontsize=12)
    ax.set_ylabel("Success Rate (%)", fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 100)

    if use_labels:
        ax.set_xticks(x_values)
        ax.set_xticklabels(x_labels, rotation=45, ha="right")

    for x_val, rate in zip(x_values, success_rates):
        ax.annotate(
            f"{rate:.1f}%",
            (x_val, rate),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
        )

    plt.tight_layout()

    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format="png", dpi=300, bbox_inches="tight")
    img_buffer.seek(0)
    img_base64 = base64.b64encode(img_buffer.getvalue()).decode()
    plt.close()

    return img_base64


def _build_table_rows(results, platform):
    """Build HTML table rows appropriate for the platform."""
    table_rows = ""
    for result in results:
        params = result.get("params", {})

        if platform == "databricks":
            table_rows += f"""
            <tr>
                <td>{result.get('series_name', 'N/A')}</td>
                <td>{result.get('experiment_id', 'N/A')}</td>
                <td>{params.get('qps', 'N/A')}</td>
                <td>{params.get('batch_size', 'N/A')}</td>
                <td>{params.get('online_store_capacity', 'N/A')}</td>
                <td>{params.get('endpoint_workload_size', 'N/A')}</td>
                <td>{params.get('num_columns', 'N/A')}</td>
                <td>{params.get('num_table_rows', 'N/A'):,}</td>
                <td>{result.get('total_requests', 0):,}</td>
                <td>{result.get('success_rate', 0):.2f}%</td>
                <td>{result.get('avg_response_time', 0):.2f}</td>
                <td>{result.get('p50_response_time', 0):.2f}</td>
                <td>{result.get('p90_response_time', 0):.2f}</td>
                <td>{result.get('p95_response_time', 0):.2f}</td>
                <td>{result.get('p99_response_time', 0):.2f}</td>
                <td>{result.get('p999_response_time', 0):.2f}</td>
            </tr>
            """
        else:
            table_rows += f"""
            <tr>
                <td>{result.get('series_name', 'N/A')}</td>
                <td>{result.get('experiment_id', 'N/A')}</td>
                <td>{params.get('task_type', 'query')}</td>
                <td>{params.get('qps', 'N/A')}</td>
                <td>{params.get('batch_size', 'N/A')}</td>
                <td>{params.get('num_columns', 'N/A')}</td>
                <td>{params.get('num_entity_keys', 'N/A'):,}</td>
                <td>{params.get('num_table_rows', 'N/A'):,}</td>
                <td>{result.get('total_requests', 0):,}</td>
                <td>{result.get('success_rate', 0):.2f}%</td>
                <td>{result.get('avg_response_time', 0):.2f}</td>
                <td>{result.get('p50_response_time', 0):.2f}</td>
                <td>{result.get('p90_response_time', 0):.2f}</td>
                <td>{result.get('p95_response_time', 0):.2f}</td>
                <td>{result.get('p99_response_time', 0):.2f}</td>
                <td>{result.get('p999_response_time', 0):.2f}</td>
            </tr>
            """
    return table_rows


def _build_table_header(platform):
    """Build HTML table header appropriate for the platform."""
    if platform == "databricks":
        return """
        <tr>
            <th>Series</th>
            <th>Experiment ID</th>
            <th>QPS</th>
            <th>Batch Size</th>
            <th>Online Store Capacity</th>
            <th>Endpoint Workload Size</th>
            <th>Num Columns</th>
            <th>Table Rows</th>
            <th>Total Requests</th>
            <th>Success Rate (%)</th>
            <th>Avg (ms)</th>
            <th>P50 (ms)</th>
            <th>P90 (ms)</th>
            <th>P95 (ms)</th>
            <th>P99 (ms)</th>
            <th>P99.9 (ms)</th>
        </tr>
        """
    else:
        return """
        <tr>
            <th>Series</th>
            <th>Experiment ID</th>
            <th>Task Type</th>
            <th>QPS</th>
            <th>Batch Size</th>
            <th>Num Columns</th>
            <th>Entity Keys</th>
            <th>Table Rows</th>
            <th>Total Requests</th>
            <th>Success Rate (%)</th>
            <th>Avg (ms)</th>
            <th>P50 (ms)</th>
            <th>P90 (ms)</th>
            <th>P95 (ms)</th>
            <th>P99 (ms)</th>
            <th>P99.9 (ms)</th>
        </tr>
        """


def generate_html_report(results, results_dir, series_charts, platform):
    """Generate comprehensive HTML report with platform-appropriate branding."""

    if platform == "databricks":
        title = "Databricks Feature Serving Load Test Report"
        accent_color = "#FF3621"
        platform_desc = "Databricks Online Feature Store + Feature Serving"
        badge_class = "platform-badge-dbx"
    else:
        title = "Snowflake Feature Store Postgres Load Test Report"
        accent_color = "#29B5E8"
        platform_desc = "Snowflake Feature Store with Postgres Online Store"
        badge_class = "platform-badge-sf"

    if not results:
        html_content = f"""
        <html>
        <head><title>{title} - No Results</title></head>
        <body>
            <h1>{title}</h1>
            <p>No test results found in the specified directory.</p>
        </body>
        </html>
        """
    else:
        series_data = {}
        for result in results:
            series_name = result.get("series_name", "unknown")
            if series_name not in series_data:
                series_data[series_name] = []
            series_data[series_name].append(result)

        series_summary = {}
        for result in results:
            series_name = result.get("series_name", "unknown")
            if series_name not in series_summary:
                series_summary[series_name] = {
                    "count": 0,
                    "total_requests": 0,
                    "avg_success_rate": 0,
                }

            series_summary[series_name]["count"] += 1
            series_summary[series_name]["total_requests"] += result.get(
                "total_requests", 0
            )
            series_summary[series_name]["avg_success_rate"] += result.get(
                "success_rate", 0
            )

        for series_name, summary in series_summary.items():
            summary["avg_success_rate"] /= summary["count"]

        summary_rows = ""
        for series_name, summary in series_summary.items():
            summary_rows += f"""
            <tr>
                <td>{series_name}</td>
                <td>{summary['count']}</td>
                <td>{summary['total_requests']:,}</td>
                <td>{summary['avg_success_rate']:.2f}%</td>
            </tr>
            """

        table_header = _build_table_header(platform)
        table_rows = _build_table_rows(results, platform)

        series_sections = ""
        for series_name in sorted(series_data.keys()):
            charts = series_charts.get(series_name, {})
            latency_chart = charts.get("latency", "")
            success_chart = charts.get("success_rate", "")

            success_section = ""
            if success_chart:
                success_section = f"""
                <div class="chart">
                    <h3>Success Rate</h3>
                    <img src="data:image/png;base64,{success_chart}" alt="{series_name} Success Rate Chart">
                </div>
                """

            series_sections += f"""
            <div class="series-section">
                <h2>{series_name}</h2>
                <div class="chart">
                    <h3>Response Time Percentiles</h3>
                    <img src="data:image/png;base64,{latency_chart}" alt="{series_name} Latency Chart">
                </div>
                {success_section}
            </div>
            """

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                h1 {{ color: #2c3e50; border-bottom: 2px solid {accent_color}; padding-bottom: 10px; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                h3 {{ color: #34495e; margin-top: 20px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: {accent_color}; color: white; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .chart {{ text-align: center; margin: 30px 0; }}
                .chart img {{ max-width: 100%; height: auto; }}
                .summary {{ background-color: #ecf0f1; padding: 20px; border-radius: 5px; margin: 20px 0; }}
                .series-section {{ margin: 40px 0; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }}
                .platform-badge-dbx {{ background-color: #FF3621; color: white; padding: 4px 12px; border-radius: 3px; font-size: 0.9em; }}
                .platform-badge-sf {{ background-color: #29B5E8; color: white; padding: 4px 12px; border-radius: 3px; font-size: 0.9em; }}
            </style>
        </head>
        <body>
            <h1>{title} <span class="{badge_class}">{platform.title()}</span></h1>

            <div class="summary">
                <h2>Test Summary</h2>
                <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Platform:</strong> {platform_desc}</p>
                <p><strong>Total Series:</strong> {len(series_summary)}</p>
                <p><strong>Total Experiments:</strong> {len(results)}</p>
                <p><strong>Total Requests:</strong> {sum(r.get('total_requests', 0) for r in results):,}</p>
            </div>

            <h2>Series Summary</h2>
            <table>
                <thead>
                    <tr>
                        <th>Series Name</th>
                        <th>Experiments</th>
                        <th>Total Requests</th>
                        <th>Avg Success Rate (%)</th>
                    </tr>
                </thead>
                <tbody>
                    {summary_rows}
                </tbody>
            </table>

            <h2>Detailed Results</h2>
            <table>
                <thead>
                    {table_header}
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>

            {series_sections}
        </body>
        </html>
        """

    report_file = os.path.join(results_dir, "final_report.html")
    with open(report_file, "w") as f:
        f.write(html_content)

    return report_file


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML report from Feature Store load test results"
    )
    parser.add_argument("results_dir", help="Path to results directory")

    args = parser.parse_args()

    if not os.path.exists(args.results_dir):
        print(f"Error: Results directory '{args.results_dir}' does not exist")
        return 1

    print(f"Loading test results from: {args.results_dir}")

    results = load_test_results(args.results_dir)

    if not results:
        print("No test results found!")
        return 1

    print(f"Found {len(results)} test results")

    platform = detect_platform(results, args.results_dir)
    print(f"Detected platform: {platform}")

    series_data = {}
    for result in results:
        series_name = result.get("series_name", "unknown")
        if series_name not in series_data:
            series_data[series_name] = []
        series_data[series_name].append(result)

    print("Generating charts...")
    series_charts = {}
    for series_name, series_results in series_data.items():
        print(f"  Generating charts for series: {series_name}")
        latency_chart = create_latency_chart_for_series(series_results)
        success_chart = create_success_rate_chart_for_series(series_results)
        series_charts[series_name] = {
            "latency": latency_chart or "",
            "success_rate": success_chart or "",
        }

    print("Generating HTML report...")
    report_file = generate_html_report(results, args.results_dir, series_charts, platform)

    print(f"Report generated successfully: {report_file}")
    print(f"Open the report in your browser to view the results")

    return 0


if __name__ == "__main__":
    exit(main())
