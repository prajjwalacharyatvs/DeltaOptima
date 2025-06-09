import requests
import json
import os
import base64
from pathlib import Path
from datetime import datetime, timedelta, timezone
import time # For current time in list_job_runs

from dotenv import load_dotenv
# Assuming utils.py is in the parent directory of pipeTimer_src, so '..'
# If pipeTimer_src is added to PYTHONPATH, then from pipeTimer_src.utils import ...
from ..utils import setup_logger


class DatabricksAPIConnector:
    def __init__(self, host=None, token=None, cluster_log_base_path=None, logger=None):
        load_dotenv() # Ensures .env variables are loaded if this class is used standalone

        resolved_host = host or os.environ.get("DATABRICKS_HOST")
        resolved_token = token or os.environ.get("DATABRICKS_TOKEN")
        self.cluster_log_base_path = cluster_log_base_path or os.environ.get("DATABRICKS_CLUSTER_LOG_BASE_PATH")

        if not resolved_host or not resolved_host.startswith("https://"):
            raise ValueError(
                "Databricks host must be provided (arg or DATABRICKS_HOST in .env) "
                "and start with https://"
            )
        if not resolved_token:
            raise ValueError(
                "Databricks API token must be provided (arg or DATABRICKS_TOKEN in .env)"
            )

        self.host = resolved_host.rstrip('/')
        self.token = resolved_token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.logger = logger or setup_logger(name="DatabricksConnector")

        if self.cluster_log_base_path:
            self.logger.info(f"Using cluster log base path: {self.cluster_log_base_path}")
        else:
            self.logger.debug("DATABRICKS_CLUSTER_LOG_BASE_PATH not set. (Note: relevant for event log download feature)")

    def _make_request(self, endpoint, method="GET", params=None, data=None, expect_json=True):
        url = f"{self.host}/api/2.0/{endpoint}"
        self.logger.debug(f"Request: {method} {url} | Params: {params} | Data: {data is not None}")
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=self.headers, params=params, timeout=60)
            elif method.upper() == "POST":
                response = requests.post(url, headers=self.headers, params=params, json=data, timeout=60)
            else:
                self.logger.error(f"Unsupported HTTP method: {method}")
                return None
            response.raise_for_status()
            if expect_json:
                return response.json()
            return response
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error: {e} | Response: {e.response.text if e.response else 'N/A'}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error: {e}")
        except json.JSONDecodeError as json_err:
            if expect_json:
                self.logger.error(f"JSON decode error: {json_err} | Response: {getattr(response, 'text', 'N/A')}")
            else:
                return response # If JSON not expected, this might be normal
        return None

    def get_cluster_details(self, cluster_id):
        """Fetches details for a specific cluster from /api/2.0/clusters/get."""
        if not cluster_id:
            self.logger.warning("Cluster ID is required to fetch cluster details.")
            return None
        self.logger.info(f"Fetching details for cluster_id: {cluster_id}")
        return self._make_request(endpoint="clusters/get", params={"cluster_id": cluster_id})

    def get_run_details(self, run_id):
        if not run_id:
            self.logger.error("Run ID is required to fetch run details.")
            return None

        self.logger.info(f"Fetching run details for run_id: {run_id}")
        data = self._make_request(endpoint="jobs/runs/get", params={"run_id": str(run_id)})

        if not data:
            return None

        notebook_path = data.get("task", {}).get("notebook_task", {}).get("notebook_path")
        if not notebook_path and data.get("job_id"):
            job_details = self.get_job_details(data.get("job_id"))
            if job_details:
                notebook_path = job_details.get("settings", {}).get("notebook_task", {}).get("notebook_path")

        enriched_cluster_spec = {}
        api_cluster_spec = data.get("cluster_spec", {})

        existing_cluster_id = api_cluster_spec.get("existing_cluster_id")
        if existing_cluster_id:
            enriched_cluster_spec["existing_cluster_id"] = existing_cluster_id
            self.logger.info(f"Run used existing cluster: {existing_cluster_id}. Fetching its full details.")
            detailed_cluster_info = self.get_cluster_details(existing_cluster_id)
            if detailed_cluster_info:
                self.logger.debug(f"Fetched details for existing cluster {existing_cluster_id}: {detailed_cluster_info}")
                enriched_cluster_spec["node_type_id"] = detailed_cluster_info.get("node_type_id")
                enriched_cluster_spec["driver_node_type_id"] = detailed_cluster_info.get("driver_node_type_id")
                enriched_cluster_spec["spark_version"] = detailed_cluster_info.get("spark_version")
                if "autoscale" in detailed_cluster_info:
                    enriched_cluster_spec["autoscale"] = detailed_cluster_info["autoscale"]
                elif "num_workers" in detailed_cluster_info:
                    enriched_cluster_spec["num_workers"] = detailed_cluster_info.get("num_workers")
            else:
                self.logger.warning(f"Could not fetch full details for existing cluster {existing_cluster_id}.")
                cluster_instance_details = data.get("cluster_instance", {})
                if cluster_instance_details: # Fallback to potentially less detailed info from instance
                    enriched_cluster_spec["node_type_id"] = enriched_cluster_spec.get("node_type_id") or cluster_instance_details.get("node_type_id")
                    enriched_cluster_spec["spark_version"] = enriched_cluster_spec.get("spark_version") or cluster_instance_details.get("spark_version")
        elif api_cluster_spec.get("new_cluster"):
            enriched_cluster_spec.update(api_cluster_spec["new_cluster"])
            self.logger.info("Run used a new job cluster.")
        
        cluster_instance = data.get("cluster_instance", {})
        if not enriched_cluster_spec and cluster_instance.get("cluster_id"): # Unlikely scenario for jobs/runs/get if spec exists
             enriched_cluster_spec["cluster_id_from_instance"] = cluster_instance.get("cluster_id")

        return {
            "run_id": data.get("run_id"),
            "job_id": data.get("job_id"),
            "run_name": data.get("run_name"),
            "notebook_path": notebook_path,
            "state": data.get("state", {}).get("life_cycle_state"),
            "result_state": data.get("state", {}).get("result_state"),
            "start_time_ms": data.get("start_time"),
            "end_time_ms": data.get("end_time"),
            "duration_ms": data.get("execution_duration"),
            "cluster_spec": enriched_cluster_spec,
            "cluster_instance": cluster_instance,
            "run_page_url": data.get("run_page_url")
        }

    def get_job_details(self, job_id):
        if not job_id:
            self.logger.error("Job ID is required to fetch job details.")
            return None
        self.logger.info(f"Fetching job details for job_id: {job_id}")
        return self._make_request(endpoint="jobs/get", params={"job_id": str(job_id)})

    def get_run_output(self, run_id):
        if not run_id:
            self.logger.error("Run ID is required to fetch run output.")
            return None
        self.logger.info(f"Fetching output for run_id: {run_id}")
        return self._make_request(endpoint="jobs/runs/get-output", params={"run_id": str(run_id)})

    def list_job_runs(self, job_id, days_history=30, result_states_filter=None):
        if not job_id:
            self.logger.error("Job ID is required to list job runs.")
            return None

        # Ensure start_time_from is timezone-aware (UTC) then convert to ms timestamp
        now_utc = datetime.now(timezone.utc)
        start_time_from_dt = now_utc - timedelta(days=days_history)
        start_time_from_ms = int(start_time_from_dt.timestamp() * 1000)
        
        all_runs_data = []
        offset = 0
        page_limit = 25 # Max limit per Databricks API documentation

        self.logger.info(f"Fetching runs for job_id {job_id} from the last {days_history} days (since {start_time_from_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}).")
        
        while True:
            params = {
                "job_id": job_id,
                "offset": offset,
                "limit": page_limit,
                "start_time_from": start_time_from_ms,
                # "completed_only": True, # For API 2.1; for 2.0, filter by life_cycle_state
                "expand_tasks": False # We don't need task details for this history
            }
            self.logger.debug(f"Requesting job runs with params: {params}")
            response_data = self._make_request(endpoint="jobs/runs/list", params=params)

            if not response_data: # Error in request
                self.logger.error(f"API error while listing runs for job_id {job_id} at offset {offset}.")
                break 
            
            current_runs_batch = response_data.get("runs", [])
            
            for run in current_runs_batch:
                life_cycle_state = run.get("state", {}).get("life_cycle_state")
                execution_duration = run.get("execution_duration")
                start_time = run.get("start_time")
                result_state = run.get("state", {}).get("result_state")

                if life_cycle_state == "TERMINATED" and execution_duration is not None and start_time is not None:
                    if result_states_filter:
                        if result_state in result_states_filter:
                             all_runs_data.append({
                                "run_id": run.get("run_id"),
                                "start_time": start_time,
                                "execution_duration": execution_duration,
                                "result_state": result_state
                            })
                    else:
                        all_runs_data.append({
                            "run_id": run.get("run_id"),
                            "start_time": start_time,
                            "execution_duration": execution_duration,
                            "result_state": result_state
                        })
            
            if response_data.get("has_more", False) and current_runs_batch: # Check if current_runs_batch is not empty
                offset += len(current_runs_batch)
            else:
                break
        
        self.logger.info(f"Found {len(all_runs_data)} relevant completed runs for job_id {job_id} in the last {days_history} days.")
        all_runs_data.sort(key=lambda r: r["start_time"]) # Sort by start time for chronological plotting
        return all_runs_data

    def download_event_logs_for_run(self, run_id, local_download_dir):
        if not self.cluster_log_base_path:
            self.logger.debug("DATABRICKS_CLUSTER_LOG_BASE_PATH not configured. Cannot download event logs.")
            return None
        
        run_details = self.get_run_details(run_id)
        if not run_details:
            self.logger.error(f"Could not get details for run {run_id} to download logs.")
            return None

        cluster_id = run_details.get("cluster_instance", {}).get("cluster_id")
        app_id_for_log_path = run_details.get("cluster_instance", {}).get("spark_context_id")

        if not cluster_id:
            self.logger.error(f"Could not determine Cluster ID for run {run_id}.")
            return None
        if not app_id_for_log_path:
            self.logger.warning(f"Could not determine Spark Context/Application ID for run {run_id}. Cannot proceed with log download.")
            return None

        dbfs_log_dir_path = f"{self.cluster_log_base_path.rstrip('/')}/{cluster_id}/spark_event_logs/{app_id_for_log_path}/"
        self.logger.info(f"Attempting to list event logs in DBFS path: {dbfs_log_dir_path}")

        list_params = {"path": dbfs_log_dir_path}
        dir_contents = self._make_request("dbfs/list", params=list_params)

        if not dir_contents or "files" not in dir_contents:
            self.logger.error(f"Could not list directory or directory is empty: {dbfs_log_dir_path}. "
                              "Check log delivery configuration and path structure.")
            return None

        event_log_files_info = [finfo for finfo in dir_contents["files"] if finfo.get("path", "").endswith(".gz")]

        if not event_log_files_info:
            self.logger.warning(f"No gzipped event log files found in {dbfs_log_dir_path}")
            return None

        downloaded_file_paths = []
        Path(local_download_dir).mkdir(parents=True, exist_ok=True)

        for file_info in event_log_files_info:
            dbfs_file_path = file_info["path"]
            file_name = os.path.basename(dbfs_file_path)
            local_file_path = Path(local_download_dir) / file_name
            
            self.logger.info(f"Downloading {dbfs_file_path} to {local_file_path}...")
            
            read_params = {"path": dbfs_file_path, "length": file_info["file_size"]} # Add length for potentially large files
            file_content_response = self._make_request("dbfs/read", params=read_params)
            
            if file_content_response and "data" in file_content_response:
                try:
                    file_data_base64 = file_content_response["data"]
                    file_data_bytes = base64.b64decode(file_data_base64)
                    
                    with open(local_file_path, "wb") as f:
                        f.write(file_data_bytes)
                    self.logger.info(f"Successfully downloaded {local_file_path}")
                    downloaded_file_paths.append(str(local_file_path))
                except Exception as e:
                    self.logger.error(f"Failed to decode or save file {dbfs_file_path}: {e}")
            else:
                self.logger.error(f"Failed to read content of {dbfs_file_path}.")
        
        return downloaded_file_paths if downloaded_file_paths else None


if __name__ == '__main__':
    print("Running DatabricksAPIConnector example...")
    example_logger = setup_logger(name="ConnectorExample", level_str="DEBUG")
    try:
        connector = DatabricksAPIConnector(logger=example_logger)
        
        test_mode = input("Test which function? (get_run_details, list_job_runs, get_run_output, download_logs): ").strip().lower()

        if test_mode == "get_run_details":
            test_run_id_input = input("Enter a Databricks Run ID: ")
            if test_run_id_input:
                run_details_output = connector.get_run_details(test_run_id_input)
                if run_details_output:
                    example_logger.info(f"\n--- Fetched Run Details (run_id {test_run_id_input}) ---")
                    print(json.dumps(run_details_output, indent=2))
        elif test_mode == "list_job_runs":
            test_job_id_input = input("Enter a Databricks Job ID: ")
            test_days_input = input("Enter number of past days for history (e.g., 30): ")
            if test_job_id_input and test_days_input:
                job_runs = connector.list_job_runs(job_id=int(test_job_id_input), days_history=int(test_days_input))
                if job_runs:
                    example_logger.info(f"\n--- Fetched Job Runs for Job ID {test_job_id_input} ---")
                    print(f"Found {len(job_runs)} runs.")
                    for i, run in enumerate(job_runs[:min(5, len(job_runs))]): # Print first 5 or less
                        print(json.dumps(run, indent=2))
                    if len(job_runs) > 5:
                        print("... and more runs ...")
                else:
                    example_logger.warning(f"No runs found or error for Job ID {test_job_id_input}.")
        elif test_mode == "get_run_output":
            test_run_id_input = input("Enter a (preferably FAILED) Databricks Run ID: ")
            if test_run_id_input:
                output_data = connector.get_run_output(test_run_id_input)
                if output_data:
                    example_logger.info(f"\n--- Fetched output for run {test_run_id_input} ---")
                    print(json.dumps(output_data, indent=2))
        elif test_mode == "download_logs":
            test_run_id_input = input("Enter a Databricks Run ID to download logs for: ")
            if test_run_id_input:
                if connector.cluster_log_base_path:
                    local_save_directory = f"./temp_event_logs_connector_test_{test_run_id_input}"
                    example_logger.info(f"Attempting to download event logs to: {local_save_directory}")
                    downloaded_logs = connector.download_event_logs_for_run(test_run_id_input, local_save_directory)
                    if downloaded_logs:
                        example_logger.info(f"Event logs downloaded: {downloaded_logs}")
                    else:
                        example_logger.warning("Log download failed or no logs found.")
                else:
                    example_logger.warning("DATABRICKS_CLUSTER_LOG_BASE_PATH not set. Cannot test log download.")
        else:
            example_logger.warning(f"Invalid test mode: {test_mode}")

    except ValueError as e:
        example_logger.error(f"Initialization error: {e}")
    except Exception as e:
        example_logger.error(f"An unexpected error: {e}", exc_info=True)