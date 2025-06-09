# pipeTimer_src/reporting/reporter.py
import json
from ..utils import setup_logger

class Reporter:
    def __init__(self, logger=None):
        self.logger = logger or setup_logger(name="Reporter")

    def generate_text_summary(self, analysis_results):
        if not analysis_results:
            return "No analysis results to report."

        self.logger.info(f"Generating text summary for run_id: {analysis_results.get('run_id')}")
        
        lines = ["--- pipeTimer Run Summary ---"]
        lines.append(f"Run ID: {analysis_results.get('run_id', 'N/A')}")
        lines.append(f"Run Name: {analysis_results.get('run_name', 'N/A')}")
        lines.append(f"Notebook Path: {analysis_results.get('notebook_path', 'N/A')}")
        if analysis_results.get('job_id', 'N/A') != 'N/A':
             lines.append(f"Databricks Job ID: {analysis_results.get('job_id')}")
        lines.append(f"Run Page URL: {analysis_results.get('run_page_url', 'N/A')}")

        status = analysis_results.get('status', {})
        lines.append("\n[Status]")
        lines.append(f"  Life Cycle State: {status.get('life_cycle_state', 'N/A')}")
        lines.append(f"  Result State: {status.get('result_state', 'N/A')}")
        
        timing = analysis_results.get('timing', {})
        lines.append("\n[Timing]")
        lines.append(f"  Start Time (UTC): {timing.get('start_time_utc', 'N/A')}")
        lines.append(f"  End Time (UTC): {timing.get('end_time_utc', 'N/A')}")
        lines.append(f"  Duration: {timing.get('duration_str', 'N/A')}")

        cluster = analysis_results.get('cluster_info', {})
        lines.append("\n[Cluster Information]")
        lines.append(f"  Type: {cluster.get('type', 'N/A')}")
        lines.append(f"  Node Type: {cluster.get('node_type', 'N/A')}")
        if cluster.get('driver_node_type') and cluster.get('driver_node_type') != 'N/A':
            lines.append(f"  Driver Node Type: {cluster.get('driver_node_type')}")
        lines.append(f"  Num Workers: {cluster.get('num_workers', 'N/A')}")
        lines.append(f"  Spark Version: {cluster.get('spark_version', 'N/A')}")

        alerts = analysis_results.get('alerts', [])
        if alerts:
            lines.append("\n[Alerts & Observations]")
            for alert in alerts:
                lines.append(f"  - {alert}")
        
        run_output_summary = analysis_results.get('run_output_summary')
        if run_output_summary:
            lines.append("\n[Run Output Diagnostics]")
            if run_output_summary.get('error'):
                lines.append(f"  Error Message: {run_output_summary['error']}")
            if run_output_summary.get('error_trace_present'):
                lines.append(f"  Error Traceback: Available in detailed logs/output (fetched via API).")
            if run_output_summary.get('notebook_result_present'):
                lines.append(f"  Notebook Result: Available in detailed logs/output (fetched via API).")
        
        lines.append("\n(Note: Spark-specific metrics like shuffle/IO require event logs, which were not processed.)")
        
        return "\n".join(lines)

    def generate_json_summary(self, analysis_results):
        # ... (no change) ...
        if not analysis_results:
            return json.dumps({"error": "No analysis results to report."}, indent=2)
        self.logger.info(f"Generating JSON summary for run_id: {analysis_results.get('run_id')}")
        return json.dumps(analysis_results, indent=2)


if __name__ == '__main__':
    # ... (keep previous __main__ block, ensure it tests with run_output_summary)
    dummy_logger = setup_logger("ReporterTest", "DEBUG")
    reporter = Reporter(logger=dummy_logger)
    
    sample_analysis_results_ok = {
        "run_id": 12345, "run_name": "Test Run", "notebook_path": "/test/notebook", "job_id": 6789,
        "status": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "is_success": True},
        "timing": {"start_time_utc": "2023-03-15 13:20:00 UTC", "end_time_utc": "2023-03-15 13:30:00 UTC", "duration_str": "600.00 seconds", "duration_ms": 600000},
        "cluster_info": {"type": "New Job Cluster", "node_type": "Standard_DS3_v2", "driver_node_type":"Standard_DS3_v2", "num_workers": "2", "spark_version": "10.4.x-scala2.12"},
        "run_page_url": "[http://example.com/run/12345](http://example.com/run/12345)",
        "alerts": []
    }
    print("--- Text Summary (Success Example) ---")
    print(reporter.generate_text_summary(sample_analysis_results_ok))

    sample_analysis_results_failed = {
        "run_id": 12346, "run_name": "Failed Test Run", "notebook_path": "/test/notebook_failed",
        "status": {"life_cycle_state": "TERMINATED", "result_state": "FAILED", "is_success": False},
        "timing": {"start_time_utc": "2023-03-15 14:00:00 UTC", "end_time_utc": "2023-03-15 14:01:00 UTC", "duration_str": "60.00 seconds", "duration_ms": 60000},
        "cluster_info": {"type": "Existing Cluster: abc-123", "node_type": "Standard_F4s", "num_workers": "Autoscaling (2-5)", "spark_version": "11.0.x-scala2.12"},
        "run_page_url": "[http://example.com/run/12346](http://example.com/run/12346)",
        "alerts": ["Run did not succeed. Result State: FAILED | Error: ValueError: Something went wrong..."],
        "run_output_summary": {
            "error": "ValueError: Something went wrong in the notebook execution.",
            "error_trace_present": True,
            "notebook_result_present": True
        }
    }
    print("\n--- Text Summary (Failure Example) ---")
    print(reporter.generate_text_summary(sample_analysis_results_failed))