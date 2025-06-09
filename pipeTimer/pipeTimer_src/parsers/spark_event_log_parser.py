# pipeTimer_src/parsers/spark_event_log_parser.py
import gzip
import json
from ..utils import setup_logger # Assuming utils.py is in the parent directory of pipeTimer_src

class SparkEventLogParser:
    def __init__(self, logger=None):
        self.logger = logger or setup_logger(name="SparkEventLogParser")
        self.parsed_data = {
            "application_info": {},
            "jobs": {},
            "stages": {}, # Basic stage info, can be expanded
            "executors": {}, # Placeholder for executor info
            "summary_metrics": {
                "total_jobs": 0,
                "successful_jobs": 0,
                "failed_jobs": 0,
                "total_stages": 0, # Will count submitted stages
                "total_tasks": 0, # Sum of tasks from submitted stages
                "aggregate_shuffle_read_bytes": 0,
                "aggregate_shuffle_write_bytes": 0,
                "aggregate_input_bytes": 0,
                "aggregate_output_bytes": 0,
            }
        }

    def _parse_event(self, event_json):
        event_type = event_json.get("Event")

        if event_type == "SparkListenerApplicationStart":
            self.parsed_data["application_info"] = {
                "app_name": event_json.get("App Name"),
                "app_id": event_json.get("App ID"),
                "start_time": event_json.get("Timestamp"),
                "spark_user": event_json.get("Spark User"),
            }
        elif event_type == "SparkListenerApplicationEnd":
            self.parsed_data["application_info"]["end_time"] = event_json.get("Timestamp")
            if self.parsed_data["application_info"].get("start_time"):
                duration = event_json.get("Timestamp") - self.parsed_data["application_info"]["start_time"]
                self.parsed_data["application_info"]["duration_ms"] = duration

        elif event_type == "SparkListenerJobStart":
            job_id = event_json.get("Job ID")
            self.parsed_data["jobs"][job_id] = {
                "job_id": job_id,
                "submission_time": event_json.get("Submission Time"),
                "stage_ids": event_json.get("Stage IDs", []),
                "status": "RUNNING"
            }
            self.parsed_data["summary_metrics"]["total_jobs"] += 1
        
        elif event_type == "SparkListenerJobEnd":
            job_id = event_json.get("Job ID")
            if job_id in self.parsed_data["jobs"]:
                self.parsed_data["jobs"][job_id]["completion_time"] = event_json.get("Completion Time")
                self.parsed_data["jobs"][job_id]["result"] = event_json.get("Job Result", {}).get("Result")
                self.parsed_data["jobs"][job_id]["status"] = event_json.get("Job Result", {}).get("Result", "UNKNOWN")
                
                if self.parsed_data["jobs"][job_id].get("submission_time"):
                    duration = event_json.get("Completion Time") - self.parsed_data["jobs"][job_id]["submission_time"]
                    self.parsed_data["jobs"][job_id]["duration_ms"] = duration
                
                if self.parsed_data["jobs"][job_id]["status"] == "JobSucceeded":
                    self.parsed_data["summary_metrics"]["successful_jobs"] +=1
                else:
                    self.parsed_data["summary_metrics"]["failed_jobs"] +=1


        elif event_type == "SparkListenerStageSubmitted":
            stage_info = event_json.get("Stage Info", {})
            stage_id = stage_info.get("Stage ID")
            self.parsed_data["stages"][stage_id] = {
                "stage_id": stage_id,
                "name": stage_info.get("Name"),
                "submission_time": stage_info.get("Submission Time"),
                "num_tasks": stage_info.get("Number of Tasks"),
                "details": stage_info.get("Details")
            }
            self.parsed_data["summary_metrics"]["total_stages"] += 1
            self.parsed_data["summary_metrics"]["total_tasks"] += stage_info.get("Number of Tasks", 0)

        elif event_type == "SparkListenerStageCompleted":
            stage_info = event_json.get("Stage Info", {})
            stage_id = stage_info.get("Stage ID")
            if stage_id in self.parsed_data["stages"]:
                self.parsed_data["stages"][stage_id]["completion_time"] = stage_info.get("Completion Time")
                self.parsed_data["stages"][stage_id]["status"] = stage_info.get("Stage Status", "UNKNOWN")
                if stage_info.get("Failure Reason"):
                     self.parsed_data["stages"][stage_id]["failure_reason"] = stage_info.get("Failure Reason")

                if self.parsed_data["stages"][stage_id].get("submission_time") and stage_info.get("Completion Time"):
                    duration = stage_info.get("Completion Time") - self.parsed_data["stages"][stage_id]["submission_time"]
                    self.parsed_data["stages"][stage_id]["duration_ms"] = duration

                # Accumulate metrics
                task_metrics = stage_info.get("Accumulables", []) # Older logs might have it here or in Task Metrics directly.
                                                                  # For newer logs, StageInfo.taskMetrics is better.
                                                                  # Let's assume we check for common patterns or prefer taskMetrics if available.
                
                # Prefer taskMetrics if available directly in StageInfo as per newer Spark versions
                stage_task_metrics = stage_info.get("Task Metrics", {})
                if stage_task_metrics:
                    self.parsed_data["summary_metrics"]["aggregate_shuffle_read_bytes"] += stage_task_metrics.get("Shuffle Read Metrics", {}).get("Remote Bytes Read", 0) + \
                                                                                           stage_task_metrics.get("Shuffle Read Metrics", {}).get("Local Bytes Read", 0)
                    self.parsed_data["summary_metrics"]["aggregate_shuffle_write_bytes"] += stage_task_metrics.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                    self.parsed_data["summary_metrics"]["aggregate_input_bytes"] += stage_task_metrics.get("Input Metrics", {}).get("Bytes Read", 0)
                    self.parsed_data["summary_metrics"]["aggregate_output_bytes"] += stage_task_metrics.get("Output Metrics", {}).get("Bytes Written", 0)
                else: # Fallback to parsing accumulables if Task Metrics is not directly present
                    for acc in task_metrics:
                        name = acc.get("Name")
                        value = acc.get("Value")
                        if isinstance(value, (int, float)): # Ensure value is numeric
                            if name == "internal.metrics.shuffle.read.remoteBytesRead" or name == "internal.metrics.shuffle.read.localBytesRead":
                                self.parsed_data["summary_metrics"]["aggregate_shuffle_read_bytes"] += value
                            elif name == "internal.metrics.shuffle.write.bytesWritten":
                                self.parsed_data["summary_metrics"]["aggregate_shuffle_write_bytes"] += value
                            elif name == "internal.metrics.input.bytesRead":
                                self.parsed_data["summary_metrics"]["aggregate_input_bytes"] += value
                            elif name == "internal.metrics.output.bytesWritten":
                                self.parsed_data["summary_metrics"]["aggregate_output_bytes"] += value
        # Add more event parsers as needed (e.g., SparkListenerTaskEnd, SparkListenerExecutorAdded)

    def parse_log_file(self, file_path):
        self.logger.info(f"Starting to parse Spark event log file: {file_path}")
        try:
            open_func = gzip.open if file_path.endswith(".gz") else open
            with open_func(file_path, 'rt', encoding='utf-8') as f:
                for line_number, line in enumerate(f, 1):
                    try:
                        event_json = json.loads(line)
                        self._parse_event(event_json)
                    except json.JSONDecodeError:
                        self.logger.warning(f"Could not decode JSON from line {line_number} in {file_path}. Skipping.")
                    except Exception as e:
                        self.logger.error(f"Error processing line {line_number} in {file_path}: {e}. Skipping.")
            self.logger.info(f"Finished parsing Spark event log file: {file_path}")
            return self.parsed_data
        except FileNotFoundError:
            self.logger.error(f"Event log file not found: {file_path}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to read or process event log file {file_path}: {e}")
            return None

    def get_summary(self):
        """Returns a summary of the parsed data."""
        # This can be expanded to be more structured
        app_duration = self.parsed_data["application_info"].get("duration_ms", "N/A")
        if isinstance(app_duration, (int, float)):
            app_duration = f"{app_duration / 1000:.2f} s"

        job_durations = []
        for job_id, job_data in self.parsed_data["jobs"].items():
            duration = job_data.get("duration_ms", "N/A")
            if isinstance(duration, (int, float)):
                 duration = f"{duration / 1000:.2f} s"
            job_durations.append({
                "job_id": job_id,
                "status": job_data.get("status", "UNKNOWN"),
                "duration": duration,
                "stage_ids": job_data.get("stage_ids")
            })
        
        return {
            "application_info": self.parsed_data["application_info"],
            "job_summary": {
                "total_jobs": self.parsed_data["summary_metrics"]["total_jobs"],
                "successful_jobs": self.parsed_data["summary_metrics"]["successful_jobs"],
                "failed_jobs": self.parsed_data["summary_metrics"]["failed_jobs"],
                "jobs": job_durations # List of jobs with their durations
            },
            "stage_summary": {
                "total_stages": self.parsed_data["summary_metrics"]["total_stages"],
                "total_tasks": self.parsed_data["summary_metrics"]["total_tasks"]
            },
            "aggregate_metrics": {
                "shuffle_read_bytes": self.parsed_data["summary_metrics"]["aggregate_shuffle_read_bytes"],
                "shuffle_write_bytes": self.parsed_data["summary_metrics"]["aggregate_shuffle_write_bytes"],
                "input_bytes": self.parsed_data["summary_metrics"]["aggregate_input_bytes"],
                "output_bytes": self.parsed_data["summary_metrics"]["aggregate_output_bytes"],
            }
        }

if __name__ == '__main__':
    # --- Example Usage ---
    # You'll need a sample gzipped Spark event log file.
    # Download one from a Databricks job run's Spark UI (Event Log tab).
    example_log_file = "path/to/your/spark_event_log.json.gz" # REPLACE THIS
    
    if example_log_file == "path/to/your/spark_event_log.json.gz":
        print("Please replace 'example_log_file' with an actual path to a Spark event log to run the example.")
    else:
        print(f"--- Testing SparkEventLogParser with: {example_log_file} ---")
        test_logger = setup_logger(name="ParserTest", level_str="INFO")
        parser = SparkEventLogParser(logger=test_logger)
        
        parsed_event_data = parser.parse_log_file(example_log_file)

        if parsed_event_data:
            summary = parser.get_summary()
            test_logger.info("--- Parser Summary ---")
            print(json.dumps(summary, indent=2))
            
            # You can also inspect the raw parsed_data if needed:
            # print("\n--- Raw Parsed Data (Application Info) ---")
            # print(json.dumps(parser.parsed_data["application_info"], indent=2))
            # print("\n--- Raw Parsed Data (Jobs Sample - first 2) ---")
            # for i, (job_id, job_data) in enumerate(parser.parsed_data["jobs"].items()):
            #     if i < 2:
            #         print(json.dumps({job_id: job_data}, indent=2))
            #     else:
            #         break
        else:
            test_logger.error("Failed to parse event log for example.")