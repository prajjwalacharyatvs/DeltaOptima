import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.align import Align
from rich.json import JSON
import os
import base64
import json 
import uuid 
from dotenv import load_dotenv

load_dotenv()

from . import auth
from . import databricks_utils
from . import api_client  
from . import output_handler

try:
    from fast_api_app.models import CodeAnalysisRequest, JobRunContext, ClusterInfo, TaskDetails
except ImportError as e:

    if 'console' not in globals():
        class DummyConsole:
            def print(self, *args, **kwargs):
                pass
        console = DummyConsole()

    console.print(f"[bold yellow][WARN] Import Error: {e}. Could not import Pydantic models from fast_api_app.models.[/bold yellow]")
    console.print("[bold yellow][WARN] Using DUMMY models for CodeAnalysisRequest and related classes in CLI.[/bold yellow]")
    console.print("[bold yellow][WARN] This is for CLI structure planning ONLY. Ensure models are correctly imported for FastAPI integration.[/bold yellow]")

    from pydantic import BaseModel, Field
    from typing import Optional, List, Dict, Any 

    class ClusterInfo(BaseModel):
        spark_version: Optional[str] = None
        node_type_id: Optional[str] = None
        driver_node_type_id: Optional[str] = None
        num_workers: Optional[int] = None
        runtime_engine: Optional[str] = None
        cloud_platform: Optional[str] = None

    class TaskDetails(BaseModel):
        task_key: str
        task_type: Optional[str] = None
        execution_duration_seconds: Optional[float] = None
        result_state: Optional[str] = None
        notebook_path: Optional[str] = None
        parameters: Optional[Dict[str, Any]] = None

    class JobRunContext(BaseModel):
        job_id: Optional[int] = None
        run_id: Optional[int] = None
        run_name: Optional[str] = None
        overall_run_duration_seconds: Optional[float] = None
        trigger_type: Optional[str] = None
        cluster_info: Optional[ClusterInfo] = None
        tasks: Optional[List[TaskDetails]] = []

    class CodeAnalysisRequest(BaseModel):
        request_id: Optional[str] = None
        code_content: str = Field(..., description="The source code of the notebook or SQL query to analyze.")
        code_language: str = Field(..., description="Language of the code (e.g., 'python', 'sql', 'scala').")
        job_context: Optional[JobRunContext] = None
# End of Pydantic model import/fallback section


# Create the Typer application
app = typer.Typer(
    name="pipeLiner",
    help="🚀 pipeLiner: Your Databricks Pipeline Optimization Assistant! 🚀\n\n"
         "This tool analyzes your Databricks notebooks and job runs to suggest "
         "code and configuration improvements for better efficiency.",
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="markdown"
)

# --- Global State ---
state = {
    "token": None,
    "host": None,
    "http_path": None,
    "verbose": False,
    "initialized_creds": False
}

console = Console() 

# --- Callback for global options ---
@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable verbose output.")] = False,
    token: Annotated[str, typer.Option(help="Databricks API Token (e.g., dapi...). Can also be set via prompt or DATABRICKS_TOKEN env var.", envvar="DATABRICKS_TOKEN")] = None,
    host: Annotated[str, typer.Option(help="Databricks Hostname (e.g., adb-xxxx.xx.azuredatabricks.net). Can also be set via prompt or DATABRICKS_HOST env var.", envvar="DATABRICKS_HOST")] = None,
    http_path: Annotated[str, typer.Option(help="Databricks SQL Warehouse ID or Cluster ID. Can also be set via prompt or DATABRICKS_HTTP_PATH env var.", envvar="DATABRICKS_HTTP_PATH")] = None,
):
    """
    pipeLiner CLI entry point.
    Manages fetching Databricks info and suggesting optimizations.
    """
    # --- BEGIN OS.GETENV DEBUG PRINT ---
    raw_env_host = os.getenv("DATABRICKS_HOST")
    raw_env_token = os.getenv("DATABRICKS_TOKEN")
    raw_env_http_path = os.getenv("DATABRICKS_HTTP_PATH")
    if verbose and ctx.invoked_subcommand : # Only print if verbose and a command is being run
        print(f"[DEBUG OS.GETENV] DATABRICKS_HOST from env: {raw_env_host}")
        print(f"[DEBUG OS.GETENV] DATABRICKS_TOKEN from env (len): {len(raw_env_token) if raw_env_token else 'None'}")
        print(f"[DEBUG OS.GETENV] DATABRICKS_HTTP_PATH from env: {raw_env_http_path}")
    # --- END OS.GETENV DEBUG PRINT ---

    if verbose:
        if not state["verbose"]:
             console.print(f"[italic blue][main_callback] Verbose mode enabled by flag.[/italic blue]")
        state["verbose"] = True
    else:
        if state["verbose"]:
            console.print("[italic blue][main_callback] Verbose mode disabled (flag not present this time).[/italic blue]")
        state["verbose"] = False

    if state["verbose"] and ctx.invoked_subcommand:
        console.print(f"[italic cyan][main_callback Typer params] host='{host}', token (len)='{len(token) if token else 0}', http_path='{http_path}'[/italic cyan]")

    if host:
        if state["verbose"] and state["host"] != host and ctx.invoked_subcommand: console.print(f"[italic blue][main_callback] Host ('{host}') being set/updated in state.[/italic blue]")
        state["host"] = host
    if token:
        if state["verbose"] and state["token"] != token and ctx.invoked_subcommand: console.print(f"[italic blue][main_callback] Token (len: {len(token) if token else 0}) being set/updated in state.[/italic blue]")
        state["token"] = token
    if http_path:
        if state["verbose"] and state["http_path"] != http_path and ctx.invoked_subcommand: console.print(f"[italic blue][main_callback] HTTP Path/SQL WH ID ('{http_path}') being set/updated in state.[/italic blue]")
        state["http_path"] = http_path

    if ctx.invoked_subcommand is None and not any(arg in ctx.args for arg in ['--help', '--no-help']):
        display_welcome_message()

def display_welcome_message():
    """Displays the styled welcome message."""
    welcome_title = Text("🚀 Welcome to the pipeLiner CLI! 🚀", style="bold white on blue", justify="center")
    info_text_line1 = Text("Your Databricks Pipeline Optimization Assistant", justify="center")
    info_text_line2 = Text("Type 'pipeLiner --help' or 'pipeLiner <command> --help' for usage details.", justify="center")

    panel_content = Text.assemble(
        welcome_title, "\n\n", info_text_line1, "\n", info_text_line2
    )
    welcome_panel = Panel(
        Align.center(panel_content),
        title="[bold cyan]pipeLiner[/bold cyan]",
        border_style="bold green",
        padding=(1, 2),
        expand=False
    )
    console.print(Align.center(welcome_panel))
    console.print()

# --- Main analysis command ---
@app.command()
def analyze(
    notebook_path_flag: Annotated[str, typer.Option("--notebook-path", help="Workspace path to a Databricks notebook (e.g., /Users/user@example.com/MyNotebook).")] = None,
    job_id_flag: Annotated[int, typer.Option("--job-id", help="Databricks Job ID. If provided without --notebook-path, pipeLiner will try to find and use the notebook from the job's latest run.")] = None,
    run_id_flag: Annotated[int, typer.Option("--run-id", help="Databricks Run ID. If provided, uses this specific run. Can be used with --job-id or to find a notebook if --notebook-path is omitted.")] = None,
    output_file: Annotated[str, typer.Option("-o", "--output", help="Path to save the analysis report (e.g., report.json).")] = None,
    token_opt: Annotated[str, typer.Option("--token", help="Databricks API Token. Overrides global/env.")] = None,
    host_opt: Annotated[str, typer.Option("--host", help="Databricks Hostname. Overrides global/env.")] = None,
    http_path_opt: Annotated[str, typer.Option("--http-path", help="Databricks SQL Warehouse ID or Cluster ID. Overrides global/env.")] = None,
):
    """
    Analyze a Databricks notebook or job run for optimization suggestions.

    You can specify the target by:
    1. --notebook-path : Directly analyze the given notebook.
                         If --job-id or --run-id is also given, their context is fetched.
    2. --job-id : Analyzes the notebook from the latest run of this job.
    3. --run-id : Analyzes the notebook from this specific run (if it's a notebook task).
    """
    if state["verbose"]:
        console.print("\n[bold dodger_blue1]Entering 'analyze' command...[/bold dodger_blue1]")

    # --- Credential Handling ---
    current_token = token_opt or state["token"]
    current_host = host_opt or state["host"]
    current_http_path = http_path_opt or state["http_path"] # Used for SQL WH ID or Cluster ID for logs

    if not (current_host and current_token): # No need to check state["initialized_creds"] here, prompt if missing
        if state["verbose"]:
            console.print(f"[italic blue][analyze] Before prompt: current_host='{current_host}', current_token (len)='{len(current_token) if current_token else 0}'[/italic blue]")
            console.print("[italic blue][analyze] Insufficient credentials found from flags/env. Prompting...[/italic blue]")
        retrieved_creds = auth.get_databricks_credentials(current_token, current_host, current_http_path)
        current_token = retrieved_creds["token"]
        current_host = retrieved_creds["host"]
        current_http_path = retrieved_creds["http_path"]
        # Update global state with potentially new/confirmed credentials
        state.update(retrieved_creds)
        state["initialized_creds"] = True # Mark that credentials have been handled in this session
    else:
        # If credentials came from flags or .env (already in state), ensure they are used
        state["token"] = current_token
        state["host"] = current_host
        state["http_path"] = current_http_path
        state["initialized_creds"] = True # Mark that credentials have been handled
        if state["verbose"]:
             console.print("[italic green][analyze] Using credentials sourced from flags or environment variables.[/italic green]")

    if not state["token"] or not state["host"]: # Final check after attempting to get credentials
        console.print("[bold red][analyze] Critical Error: Databricks host and token are required but not available.[/bold red]")
        raise typer.Exit(1)


    if state["verbose"]:
        console.print("\n[bold magenta]Current Configuration for Analysis:[/bold magenta]")
        console.print(f"  Databricks Host: {state['host']}")
        console.print(f"  Databricks Token: {'******' if state['token'] else 'Not set'}")
        console.print(f"  Databricks HTTP Path/SQL WH ID: {state['http_path'] if state['http_path'] else 'Not set'}")
        console.print(f"  Flag --notebook-path: {notebook_path_flag}")
        console.print(f"  Flag --job-id: {job_id_flag}")
        console.print(f"  Flag --run-id: {run_id_flag}")
        console.print(f"  Output File: {output_file}")

    if not notebook_path_flag and not job_id_flag and not run_id_flag:
        console.print("\n[bold red][ERROR][/bold red] Please specify target using --notebook-path, --job-id, or --run-id.")
        console.print("Use '[cyan]pipeLiner analyze --help[/cyan]' for more information.")
        raise typer.Exit(code=1)

    console.print("\n[INFO] Credentials processed. Proceeding to fetch Databricks data...")

    # --- Data Fetching Logic ---
    fetched_run_details_dict: Optional[dict] = None
    actual_notebook_path_to_fetch: Optional[str] = notebook_path_flag
    fetched_notebook_source_code: Optional[str] = None
    notebook_file_type: str = "unknown"

    # Step 1: Fetch Job/Run details if IDs are provided
    if run_id_flag:
        console.print(f"\n[cyan]--- Fetching Job Run Details for specific Run ID: {run_id_flag} ---[/cyan]")
        fetched_run_details_dict = databricks_utils.get_run_details(
            host=state["host"], token=state["token"], run_id=run_id_flag, verbose=state["verbose"]
        )
    elif job_id_flag:
        console.print(f"\n[cyan]--- Fetching Latest Job Run Details for Job ID: {job_id_flag} ---[/cyan]")
        fetched_run_details_dict = databricks_utils.get_latest_run_for_job(
            host=state["host"], token=state["token"], job_id=job_id_flag, verbose=state["verbose"]
        )

    if fetched_run_details_dict:
        console.print(f"[green]Successfully fetched job run details (Run ID: {fetched_run_details_dict.get('run_id', 'N/A')}).[/green]")
        if state["verbose"]:
            console.print(f"[bold]Job Run Details JSON:[/bold]")
            console.print(JSON(json.dumps(fetched_run_details_dict))) # Pretty print JSON

        if not actual_notebook_path_to_fetch: # Try to discover notebook from job if not given by flag
            console.print("[italic blue]No --notebook-path provided. Attempting to discover notebook from job tasks...[/italic blue]")
            tasks_raw = fetched_run_details_dict.get("tasks", [])
            for task_info_raw in tasks_raw:
                if task_info_raw.get("notebook_task") and task_info_raw["notebook_task"].get("notebook_path"):
                    actual_notebook_path_to_fetch = task_info_raw["notebook_task"]["notebook_path"]
                    console.print(f"[green]  Discovered notebook path from task '{task_info_raw.get('task_key')}': {actual_notebook_path_to_fetch}[/green]")
                    break # Use the first notebook task found
            if not actual_notebook_path_to_fetch:
                console.print("[yellow]  Could not discover a notebook path from the job run tasks.[/yellow]")

    # Step 2: Fetch Notebook Content if we have a path (either from flag or discovered)
    if actual_notebook_path_to_fetch:
        console.print(f"\n[cyan]--- Fetching Notebook Content for: {actual_notebook_path_to_fetch} ---[/cyan]")
        notebook_export_data = databricks_utils.get_notebook_content(
            host=state["host"], token=state["token"], notebook_path=actual_notebook_path_to_fetch, verbose=state["verbose"]
        )
        if notebook_export_data and "content" in notebook_export_data:
            try:
                fetched_notebook_source_code = base64.b64decode(notebook_export_data["content"]).decode('utf-8')
                notebook_file_type = notebook_export_data.get("file_type", "unknown").lower()
                console.print(f"[green]Successfully decoded notebook content (type: {notebook_file_type}).[/green]")
                if state["verbose"]:
                    console.print(f"[bold]Notebook Source (first 500 chars):[/bold]\n{fetched_notebook_source_code[:500]}...")
            except Exception as e:
                console.print(f"[bold red]Error decoding notebook content for '{actual_notebook_path_to_fetch}': {e}[/bold red]")
                if state["verbose"] and notebook_export_data: console.print(JSON(json.dumps(notebook_export_data)))
        elif notebook_export_data:
             console.print(f"[yellow]Notebook data for '{actual_notebook_path_to_fetch}' fetched but 'content' was missing or in unexpected format.[/yellow]")
             if state["verbose"]: console.print(JSON(json.dumps(notebook_export_data)))
        else:
            console.print(f"[red]Failed to fetch or process notebook content for '{actual_notebook_path_to_fetch}'.[/red]")
    elif notebook_path_flag: # User specified a path, but it wasn't used (e.g., fetching failed earlier and actual_notebook_path_to_fetch became None)
         console.print(f"[yellow]Notebook path '{notebook_path_flag}' was provided via flag but content could not be fetched or processed.[/yellow]")


    # --- Code Selection and Language Determination ---
    code_to_analyze: Optional[str] = None
    code_language: str = "unknown"
    analysis_target_description: str = "N/A"

    if fetched_notebook_source_code:
        code_to_analyze = fetched_notebook_source_code
        # notebook_file_type is already .lower()'d, e.g., 'python', 'sql', 'py'
        if notebook_file_type == "python" or notebook_file_type == "py": # <---MODIFIED HERE
            code_language = "python_spark"
        elif notebook_file_type == "sql":
            code_language = "sql"
        elif notebook_file_type == "scala": # API returns SCALA
            code_language = "scala_spark"
        elif notebook_file_type == "r": # API returns R
            code_language = "r_spark"
        else:
            code_language = notebook_file_type # Fallback
        analysis_target_description = f"Notebook: {actual_notebook_path_to_fetch or notebook_path_flag} (type: {code_language})"
    # TODO: Add logic for 'extracted_sql_queries' if a job has SQL tasks and no notebook_path was focused on.
    # For now, we prioritize notebook code if found/specified.

    if not code_to_analyze:
        console.print("\n[bold red]No primary code (notebook or extracted SQL) available to analyze. Exiting.[/bold red]")
        if job_id_flag or run_id_flag:
            console.print("[bold red]  If analyzing a job/run, ensure it contains a recognizable notebook task, or that --notebook-path is correctly specified for the job's notebook.[/bold red]")
        elif notebook_path_flag:
            console.print(f"[bold red]  The specified notebook path '{notebook_path_flag}' could not be fetched or processed.[/bold red]")
        raise typer.Exit(code=1)

    console.print(f"\n[INFO] Primary analysis target: {analysis_target_description}")

    # --- Structure data for Analysis Request ---
    console.print("\n[INFO] Structuring data for analysis request...")

    job_run_context_for_api: Optional[JobRunContext] = None
    if fetched_run_details_dict:
        cluster_info_for_api: Optional[ClusterInfo] = None
        # Check for new cluster defined in job_clusters
        job_clusters_raw = fetched_run_details_dict.get("job_clusters", [])
        # Check for existing cluster specified in cluster_spec (often directly under run or task)
        cluster_spec_raw = fetched_run_details_dict.get("cluster_spec") 
        # Or sometimes under task's existing_cluster_id then you might need another API call to get cluster details
        # For simplicity, we handle new_cluster and top-level cluster_spec.
        
        cluster_config_source = None
        if job_clusters_raw and "new_cluster" in job_clusters_raw[0]:
            cluster_config_source = job_clusters_raw[0]["new_cluster"]
        elif cluster_spec_raw: # If the job ran on an existing cluster (details in cluster_spec)
             cluster_config_source = cluster_spec_raw

        if cluster_config_source:
            cloud_platform = None
            if cluster_config_source.get("azure_attributes"): cloud_platform = "azure"
            elif cluster_config_source.get("aws_attributes"): cloud_platform = "aws"
            
            cluster_info_for_api = ClusterInfo(
                spark_version=cluster_config_source.get("spark_version"),
                node_type_id=cluster_config_source.get("node_type_id"),
                driver_node_type_id=cluster_config_source.get("driver_node_type_id", cluster_config_source.get("node_type_id")), # Default to worker if not specified
                num_workers=cluster_config_source.get("num_workers"),
                runtime_engine=cluster_config_source.get("runtime_engine"),
                cloud_platform=cloud_platform
            )

        tasks_for_api: List[TaskDetails] = []
        tasks_raw = fetched_run_details_dict.get("tasks", [])
        for task_info_raw in tasks_raw:
            task_type = "unknown"
            nb_path_for_task = None
            params_for_task = None

            if task_info_raw.get("notebook_task"):
                task_type = "notebook_task"
                nb_path_for_task = task_info_raw["notebook_task"].get("notebook_path")
                params_for_task = task_info_raw["notebook_task"].get("base_parameters")
            elif task_info_raw.get("sql_task"):
                task_type = "sql_task"
                # params_for_task = task_info_raw["sql_task"].get("parameters") # Example, actual structure may vary
            # Add more task types like spark_python_task, spark_jar_task, python_wheel_task etc.
            elif task_info_raw.get("spark_python_task"):
                task_type = "spark_python_task"
                # params_for_task = task_info_raw["spark_python_task"].get("parameters")
            # ... and so on for other relevant task types

            tasks_for_api.append(
                TaskDetails(
                    task_key=task_info_raw.get("task_key"),
                    task_type=task_type,
                    execution_duration_seconds=(task_info_raw.get("execution_duration", 0) / 1000.0) if task_info_raw.get("execution_duration") else None,
                    result_state=task_info_raw.get("state", {}).get("result_state"),
                    notebook_path=nb_path_for_task,
                    parameters=params_for_task
                )
            )

        job_run_context_for_api = JobRunContext(
            job_id=fetched_run_details_dict.get("job_id"),
            run_id=fetched_run_details_dict.get("run_id"),
            run_name=fetched_run_details_dict.get("run_name"),
            overall_run_duration_seconds=(fetched_run_details_dict.get("run_duration", 0) / 1000.0) if fetched_run_details_dict.get("run_duration") else None,
            trigger_type=fetched_run_details_dict.get("trigger"),
            cluster_info=cluster_info_for_api,
            tasks=tasks_for_api
        )

    analysis_request_payload = CodeAnalysisRequest(
        request_id=str(uuid.uuid4()),
        code_content=code_to_analyze, # This is guaranteed to be non-None if we reached here
        code_language=code_language,
        job_context=job_run_context_for_api
    )

    if state["verbose"]:
        console.print("\n[bold blue]--- Data Payload for Analysis API (to be sent to FastAPI) ---[/bold blue]")
        try:
            console.print(JSON(analysis_request_payload.model_dump_json(indent=2)))
        except AttributeError: # Fallback for Pydantic v1
             console.print(JSON(analysis_request_payload.json(indent=2)))
    console.print("\n[INFO] Data structuring complete.")

    # --- Call the Analysis API ---
    console.print("\n[INFO] Sending data to analysis service...")
    
    # Convert Pydantic model to dict for the requests library
    # (api_client.py expects a dict for the 'json' parameter of requests.post)
    payload_dict = analysis_request_payload.model_dump() 
    
    api_response = api_client.call_analysis_api(
        payload=payload_dict,
        # api_url can be made configurable later if needed, e.g., from .env
        verbose=state["verbose"]
    )

    # --- Handle and Display API Response ---
    if api_response:
        console.print("[green]Successfully received analysis from the service.[/green]")
        output_handler.display_analysis_results(api_response, verbose=state["verbose"])
        if output_file:
            output_handler.save_analysis_results(api_response, output_file, verbose=state["verbose"])
    else:
        console.print("[bold red]Failed to get a valid response from the analysis service.[/bold red]")
        console.print("[bold red]Please check the FastAPI server logs for more details if it's running.[/bold red]")

    console.print("\n[INFO] pipeLiner analysis process finished.")


# --- Configure command (remains the same) ---
@app.command()
def configure():
    """
    (Placeholder) Configure pipeLiner settings, like saving default credentials.
    """
    console.print("\n[bold yellow](Placeholder)[/bold yellow] This is a 'configure' command.")
    console.print("Future functionality: Save and manage Databricks connection details securely.")

if __name__ == "__main__":
    app()