import datetime as dt_module
import json
from collections import Counter
import statistics # For median, stdev if needed
import numpy as np # For stdev, mean, min, max easily
from ..utils import setup_logger

class AnalysisEngine:
    def __init__(self, logger=None):
        self.logger = logger or setup_logger(name="AnalysisEngine")

    def _format_duration(self, milliseconds):
        # ... (no change from previous version)
        if milliseconds is None:
            return "N/A"
        seconds = milliseconds / 1000.0 
        if seconds < 0: 
            return "Invalid duration"
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds_val = divmod(remainder, 60)
        parts = []
        if days > 0: parts.append(f"{int(days)}d")
        if hours > 0: parts.append(f"{int(hours)}h")
        if minutes > 0: parts.append(f"{int(minutes)}m")
        if seconds_val > 0 or not parts or (days == 0 and hours == 0 and minutes == 0):
            parts.append(f"{seconds_val:.2f}s")
        return " ".join(parts) if parts else "0.00s"

    def _format_timestamp(self, timestamp_ms, include_tz=True):
        if timestamp_ms is None:
            return "N/A"
        dt_obj = dt_module.datetime.fromtimestamp(timestamp_ms / 1000, tz=dt_module.timezone.utc)
        if include_tz:
            return dt_obj.strftime('%Y-%m-%d %H:%M:%S %Z')
        return dt_obj.strftime('%Y-%m-%d %H:%M:%S')

    def process_job_details(self, job_api_data):
        if not job_api_data:
            self.logger.warning("No job API data provided to process.")
            return None
        
        settings = job_api_data.get("settings", {})
        schedule = settings.get("schedule", {})
        
        tasks_summary = []
        for i, task in enumerate(settings.get("tasks", [])):
            task_key = task.get("task_key", f"Task_{i+1}")
            task_desc = f"Key: {task_key}"
            if "notebook_task" in task:
                task_desc += f", Notebook: {task['notebook_task'].get('notebook_path', 'N/A')}"
            elif "spark_jar_task" in task:
                task_desc += f", JAR Main Class: {task['spark_jar_task'].get('main_class_name', 'N/A')}"
            # Add other task types if needed
            tasks_summary.append(task_desc)

        cluster_info = "N/A"
        if settings.get("new_cluster"):
            nc = settings["new_cluster"]
            cluster_info = (f"New Job Cluster: {nc.get('spark_version', 'N/A')}, "
                            f"{nc.get('node_type_id', 'N/A')}, "
                            f"Workers: {nc.get('num_workers', 'N/A') if not nc.get('autoscale') else f'Autoscale {nc.get('autoscale').get('min_workers')}-{nc.get('autoscale').get('max_workers')}'}")
        elif settings.get("existing_cluster_id"):
            cluster_info = f"Existing Cluster ID: {settings['existing_cluster_id']}"

        email_notifications = settings.get("email_notifications", {})
        notifications_summary = []
        if email_notifications.get("on_start"): notifications_summary.append(f"On Start: {', '.join(email_notifications['on_start'])}")
        if email_notifications.get("on_success"): notifications_summary.append(f"On Success: {', '.join(email_notifications['on_success'])}")
        if email_notifications.get("on_failure"): notifications_summary.append(f"On Failure: {', '.join(email_notifications['on_failure'])}")


        job_details_summary = {
            "job_id": job_api_data.get("job_id"),
            "name": settings.get("name", "N/A"),
            "creator_user_name": job_api_data.get("creator_user_name", "N/A"),
            "created_time_utc": self._format_timestamp(job_api_data.get("created_time")),
            "schedule_cron": schedule.get("quartz_cron_expression", "Not Scheduled"),
            "schedule_timezone": schedule.get("timezone_id", "N/A"),
            "tasks_summary": tasks_summary or ["No tasks defined"],
            "default_cluster_info": cluster_info,
            "libraries": settings.get("libraries", []), # List of library objects
            "max_concurrent_runs": settings.get("max_concurrent_runs", "N/A"),
            "timeout_seconds": settings.get("timeout_seconds", "N/A"),
            "retry_on_timeout": settings.get("retry_on_timeout", False),
            "notification_summary": notifications_summary or ["None configured"],
            "tags": settings.get("tags", {})
        }
        self.logger.info(f"Processed details for job_id: {job_api_data.get('job_id')}")
        return job_details_summary

    def analyze_run_history(self, job_runs_raw):
        """
        Analyzes a list of job runs to extract statistics and identify notable runs.
        Args:
            job_runs_raw (list): List of run dicts from DatabricksAPIConnector.list_job_runs
                                 (expects 'start_time', 'execution_duration', 'result_state', 'run_id')
        """
        if not job_runs_raw:
            self.logger.info("No job runs provided for history analysis.")
            return None

        num_runs = len(job_runs_raw)
        self.logger.info(f"Analyzing run history for {num_runs} runs.")

        durations_ms = [run['execution_duration'] for run in job_runs_raw if run.get('execution_duration') is not None]
        start_times_ms = [run['start_time'] for run in job_runs_raw if run.get('start_time') is not None]
        
        result_states = [run.get('result_state', 'UNKNOWN') for run in job_runs_raw]
        status_counts = Counter(result_states)
        successful_runs = status_counts.get("SUCCESS", 0)
        success_rate = (successful_runs / num_runs * 100) if num_runs > 0 else 0

        analysis = {
            "total_runs_analyzed": num_runs,
            "period_start_utc": self._format_timestamp(min(start_times_ms)) if start_times_ms else "N/A",
            "period_end_utc": self._format_timestamp(max(start_times_ms)) if start_times_ms else "N/A",
            "duration_stats_ms": {},
            "duration_stats_str": {},
            "success_rate_percent": f"{success_rate:.2f}%",
            "status_counts": dict(status_counts),
            "notable_runs": {
                "longest": [],
                "shortest": [],
                "recent_failures": []
            }
        }

        if durations_ms:
            durations_arr_ms = np.array(durations_ms)
            analysis["duration_stats_ms"] = {
                "min": float(np.min(durations_arr_ms)),
                "max": float(np.max(durations_arr_ms)),
                "mean": float(np.mean(durations_arr_ms)),
                "median": float(np.median(durations_arr_ms)),
                "std_dev": float(np.std(durations_arr_ms))
            }
            analysis["duration_stats_str"] = {k: self._format_duration(v) for k, v in analysis["duration_stats_ms"].items()}
        
        # Sort runs by duration for longest/shortest
        runs_sorted_by_duration = sorted(job_runs_raw, key=lambda r: r.get('execution_duration', 0), reverse=True)
        for i in range(min(3, len(runs_sorted_by_duration))):
            run = runs_sorted_by_duration[i]
            analysis["notable_runs"]["longest"].append({
                "run_id": run.get("run_id"),
                "duration_str": self._format_duration(run.get("execution_duration")),
                "start_time_utc": self._format_timestamp(run.get("start_time"), include_tz=False), # Shorter for table
                "result_state": run.get("result_state")
            })
        
        runs_sorted_by_duration.reverse() # Now shortest first
        for i in range(min(3, len(runs_sorted_by_duration))):
            run = runs_sorted_by_duration[i]
            analysis["notable_runs"]["shortest"].append({
                "run_id": run.get("run_id"),
                "duration_str": self._format_duration(run.get("execution_duration")),
                "start_time_utc": self._format_timestamp(run.get("start_time"), include_tz=False),
                "result_state": run.get("result_state")
            })

        # Sort runs by start time (descending) for recent failures
        runs_sorted_by_time_desc = sorted(job_runs_raw, key=lambda r: r.get('start_time', 0), reverse=True)
        failed_count = 0
        for run in runs_sorted_by_time_desc:
            if run.get('result_state') != "SUCCESS" and run.get('result_state') != "UNKNOWN": # Consider any non-success a failure here
                if failed_count < 3:
                    analysis["notable_runs"]["recent_failures"].append({
                        "run_id": run.get("run_id"),
                        "duration_str": self._format_duration(run.get("execution_duration")),
                        "start_time_utc": self._format_timestamp(run.get("start_time"), include_tz=False),
                        "result_state": run.get("result_state")
                    })
                    failed_count += 1
                else:
                    break
        
        return analysis

    def process_run_details_for_report(self, run_api_data, run_output_data=None):
        # ... (This method remains the same as before) ...
        # ... (ensure it's the complete method from the previous step) ...
        if not run_api_data:
            self.logger.warning("No API run data provided to process for individual report.")
            return None
        # (Keep the full implementation for process_run_details_for_report)
        self.logger.info(f"Processing API data for run_id: {run_api_data.get('run_id')}")
        cluster_spec = run_api_data.get('cluster_spec', {})
        cluster_type_display = "N/A"; num_workers_display = "N/A"
        if cluster_spec.get("existing_cluster_id"):
            cluster_type_display = f"Existing Cluster: {cluster_spec['existing_cluster_id']}"
            if "num_workers" in cluster_spec: num_workers_display = str(cluster_spec["num_workers"])
            elif "autoscale" in cluster_spec: 
                min_w = cluster_spec["autoscale"].get("min_workers", "N/A"); max_w = cluster_spec["autoscale"].get("max_workers", "N/A")
                num_workers_display = f"Autoscaling ({min_w}-{max_w})"
            else: num_workers_display = "N/A (details not fully resolved)"
        elif cluster_spec: 
            cluster_type_display = "New Job Cluster"; num_workers_display = str(cluster_spec.get('num_workers', 'N/A'))
        summary = {
            "run_id": run_api_data.get('run_id', 'N/A'), "run_name": run_api_data.get('run_name', 'N/A'),
            "notebook_path": run_api_data.get('notebook_path', 'N/A'),
            "status": {"life_cycle_state": run_api_data.get('state', 'N/A'), "result_state": run_api_data.get('result_state', 'N/A'),
                       "is_success": str(run_api_data.get('result_state', '')).upper() == "SUCCESS"},
            "timing": {"start_time_utc": self._format_timestamp(run_api_data.get('start_time_ms')), 
                       "end_time_utc": self._format_timestamp(run_api_data.get('end_time_ms')),
                       "duration_str": self._format_duration(run_api_data.get('duration_ms')), "duration_ms": run_api_data.get('duration_ms')},
            "cluster_info": {"type": cluster_type_display, "node_type": cluster_spec.get('node_type_id', 'N/A'),
                             "driver_node_type": cluster_spec.get('driver_node_type_id', 'N/A'),
                             "num_workers": num_workers_display, "spark_version": cluster_spec.get('spark_version', 'N/A')},
            "run_page_url": run_api_data.get('run_page_url', 'N/A'), "job_id": run_api_data.get('job_id', 'N/A')}
        summary["alerts"] = []
        if not summary["status"]["is_success"]:
            alert_msg = f"Run did not succeed. Result State: {summary['status']['result_state']}"
            if run_output_data and run_output_data.get("error"):
                error_msg = run_output_data.get('error', ''); alert_msg += f" | Error: {error_msg[:200]}{'...' if len(error_msg) > 200 else ''}"
            summary["alerts"].append(alert_msg)
        if run_api_data.get('duration_ms', 0) > (2 * 60 * 60 * 1000): 
             summary["alerts"].append(f"Run duration ({summary['timing']['duration_str']}) is significant.")
        if run_output_data:
            summary["run_output_summary"] = {}
            if run_output_data.get("error"): summary["run_output_summary"]["error_message"] = run_output_data.get("error")
            if run_output_data.get("error_trace"): summary["run_output_summary"]["error_trace_present"] = True
            notebook_output = run_output_data.get("notebook_output", {});
            if notebook_output.get("result"):
                 summary["run_output_summary"]["notebook_result_present"] = True
                 summary["run_output_summary"]["notebook_output_truncated"] = notebook_output.get("truncated", False)
            if run_output_data.get("logs_truncated") is not None: 
                 summary["run_output_summary"]["standard_logs_truncated"] = run_output_data.get("logs_truncated")
            self.logger.debug(f"Included run output summary for run {summary['run_id']}")
        return summary


    def prepare_runs_for_plotting(self, job_runs):
        # ... (no change from previous version) ...
        if not job_runs: self.logger.info("No job runs to prepare for plotting."); return []
        prepared_runs = []
        for run in job_runs:
            if 'start_time' in run and 'execution_duration' in run:
                prepared_runs.append({
                    'start_time_dt': dt_module.datetime.fromtimestamp(run['start_time'] / 1000, tz=dt_module.timezone.utc),
                    'duration_minutes': run['execution_duration'] / (1000.0 * 60.0) 
                })
            else: self.logger.warning(f"Skipping run {run.get('run_id', 'N/A')} for plotting due to missing time/duration.")
        self.logger.info(f"Prepared {len(prepared_runs)} runs for plotting.")
        return prepared_runs

if __name__ == '__main__':
    dummy_logger = setup_logger("EngineTest", "DEBUG")
    engine = AnalysisEngine(logger=dummy_logger)
    
    # Test process_job_details
    sample_job_api_data = {
        "job_id": 123, "creator_user_name": "test@example.com", 
        "created_time": dt_module.datetime.now(dt_module.timezone.utc).timestamp() * 1000,
        "settings": {
            "name": "My Awesome ETL Job", 
            "tags": {"department": "finance", "criticality": "high"},
            "schedule": {"quartz_cron_expression": "0 0 5 * * ?", "timezone_id": "America/New_York"},
            "tasks": [
                {"task_key": "ingest_data", "notebook_task": {"notebook_path": "/Users/test/IngestNotebook"}},
                {"task_key": "transform_data", "notebook_task": {"notebook_path": "/Users/test/TransformNotebook"}}
            ],
            "new_cluster": {"spark_version": "10.4.x-scala2.12", "node_type_id": "i3.xlarge", "num_workers": 5},
            "libraries": [{"jar": "dbfs:/path/to/my.jar"}],
            "max_concurrent_runs": 3, "timeout_seconds": 7200, "retry_on_timeout": True,
            "email_notifications": {"on_failure": ["admin@example.com"], "on_success": ["user@example.com"]}
        }
    }
    processed_job_details = engine.process_job_details(sample_job_api_data)
    print("--- Processed Job Details (Enhanced) ---")
    print(json.dumps(processed_job_details, indent=2))

    # Test analyze_run_history
    sample_runs_for_history = [
        {"run_id": 101, "start_time": (dt_module.datetime.now(dt_module.timezone.utc) - dt_module.timedelta(days=3)).timestamp() * 1000, "execution_duration": 120000, "result_state": "SUCCESS"}, # 2 min
        {"run_id": 102, "start_time": (dt_module.datetime.now(dt_module.timezone.utc) - dt_module.timedelta(days=2)).timestamp() * 1000, "execution_duration": 180000, "result_state": "SUCCESS"}, # 3 min
        {"run_id": 103, "start_time": (dt_module.datetime.now(dt_module.timezone.utc) - dt_module.timedelta(days=1)).timestamp() * 1000, "execution_duration": 60000, "result_state": "FAILED"},   # 1 min
        {"run_id": 104, "start_time": (dt_module.datetime.now(dt_module.timezone.utc) - dt_module.timedelta(hours=5)).timestamp() * 1000, "execution_duration": 300000, "result_state": "SUCCESS"},# 5 min
        {"run_id": 105, "start_time": (dt_module.datetime.now(dt_module.timezone.utc) - dt_module.timedelta(hours=2)).timestamp() * 1000, "execution_duration": 240000, "result_state": "CANCELED"},# 4 min
    ]
    history_analysis = engine.analyze_run_history(sample_runs_for_history)
    print("\n--- Run History Analysis ---")
    print(json.dumps(history_analysis, indent=2))
    
    # ... (existing tests for process_run_details_for_report and prepare_runs_for_plotting) ...