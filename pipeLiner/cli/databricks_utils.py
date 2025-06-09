import requests
from rich.console import Console
from rich.json import JSON
import json

console = Console()


def _get_api_headers(token: str) -> dict:
    """Helper function to construct standard API headers."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

def _make_databricks_request(host: str, token: str, endpoint: str, method: str = "GET", params: dict = None, json_payload: dict = None, verbose: bool = False) -> dict | None:
    """
    Helper function to make a generic request to the Databricks API.
    Returns the JSON response as a dictionary or None if an error occurs.
    """
    if not host or not token:
        console.print("[bold red]Error: Databricks host and token must be provided.[/bold red]")
        return None

    full_url = f"https://{host.rstrip('/')}{endpoint}"
    headers = _get_api_headers(token)

    if verbose:
        console.print(f"[italic blue]Making Databricks API call:[/italic blue]")
        console.print(f"  Method : {method}")
        console.print(f"  URL    : {full_url}")
        if params:
            console.print(f"  Params : {params}")
        if json_payload:
            console.print(f"  Payload: {json_payload}")

    try:
        if method.upper() == "GET":
            response = requests.get(full_url, headers=headers, params=params, timeout=30)
        elif method.upper() == "POST":
            response = requests.post(full_url, headers=headers, params=params, json=json_payload, timeout=30)
        # Add other methods (PUT, DELETE) if needed later
        else:
            console.print(f"[bold red]Unsupported HTTP method: {method}[/bold red]")
            return None

        response.raise_for_status()  
        return response.json()

    except requests.exceptions.HTTPError as errh:
        console.print(f"[bold red]Http Error:[/bold red] {errh}")
        if errh.response is not None and errh.response.text:
            try:
                error_details_dict = errh.response.json()
                console.print("[bold red]Error Details:[/bold red]")
                console.print(JSON(json.dumps(error_details_dict)))
            except ValueError:
                console.print(f"[bold red]Error Response (non-JSON):[/bold red] {errh.response.text[:500]}")
    except requests.exceptions.ConnectionError as errc:
        console.print(f"[bold red]Error Connecting:[/bold red] {errc}")
    except requests.exceptions.Timeout as errt:
        console.print(f"[bold red]Timeout Error:[/bold red] {errt}")
    except requests.exceptions.RequestException as err:
        console.print(f"[bold red]OOps: Something Else Went Wrong with the Request[/bold red] {err}")
    except ValueError as json_err:
        console.print(f"[bold red]JSON Decode Error:[/bold red] Failed to parse response from {full_url}. Response: {response.text[:200] if response else 'N/A'}")
    return None


def get_notebook_content(host: str, token: str, notebook_path: str, verbose: bool = False) -> dict | None:
    """
    Fetches the content of a Databricks notebook.
    Uses /api/2.0/workspace/export API.
    """
    if not notebook_path:
        console.print("[bold red]Error: Notebook path is required.[/bold red]")
        return None

    endpoint = "/api/2.0/workspace/export"
    params = {
        "path": notebook_path,
        "format": "SOURCE"
    }
    if verbose:
        console.print(f"[italic blue]Attempting to fetch notebook content for: {notebook_path}[/italic blue]")

    response_data = _make_databricks_request(host, token, endpoint, method="GET", params=params, verbose=verbose)

    if response_data and "content" in response_data:
        if verbose:
            console.print(f"[green]Successfully fetched notebook content (format: {response_data.get('file_type', 'N/A')}).[/green]")
        return response_data 
    elif response_data:
        console.print(f"[yellow]Warning: Fetched data for notebook '{notebook_path}' but 'content' key is missing.[/yellow]")
        if verbose: console.print(JSON(response_data))
        return response_data
    return None

def get_run_details(host: str, token: str, run_id: int, verbose: bool = False) -> dict | None:
    """
    Fetches details for a specific job run using its run_id.
    Uses /api/2.1/jobs/runs/get API.
    """
    if not run_id:
        console.print("[bold red]Error: Run ID is required.[/bold red]")
        return None

    endpoint = "/api/2.1/jobs/runs/get"
    params = {"run_id": run_id}
    if verbose:
        console.print(f"[italic blue]Attempting to fetch details for run ID: {run_id}[/italic blue]")

    response_data = _make_databricks_request(host, token, endpoint, method="GET", params=params, verbose=verbose)

    if response_data:
        if verbose:
            console.print(f"[green]Successfully fetched details for run ID: {run_id}.[/green]")
        return response_data
    return None


def get_latest_run_for_job(host: str, token: str, job_id: int, verbose: bool = False) -> dict | None:
    """
    Fetches the latest run for a given job_id.
    Uses /api/2.1/jobs/runs/list and then /api/2.1/jobs/runs/get.
    Returns details of the latest completed run if available.
    """
    if not job_id:
        console.print("[bold red]Error: Job ID is required.[/bold red]")
        return None

    list_endpoint = "/api/2.1/jobs/runs/list"
    list_params = {
        "job_id": job_id,
        "order_by": "START_TIME_DESC",
        "limit": 5
    }
    if verbose:
        console.print(f"[italic blue]Attempting to list runs for job ID: {job_id} to find the latest.[/italic blue]")

    list_response = _make_databricks_request(host, token, list_endpoint, method="GET", params=list_params, verbose=verbose)

    if list_response and "runs" in list_response and list_response["runs"]:
        latest_run_info = list_response["runs"][0]
        latest_run_id = latest_run_info.get("run_id")
        if latest_run_id:
            if verbose:
                console.print(f"[italic blue]Found latest run attempt ID: {latest_run_id} for job ID: {job_id}. Fetching its details...[/italic blue]")
            return get_run_details(host, token, latest_run_id, verbose)
        else:
            console.print(f"[yellow]Warning: No 'run_id' found in the latest run info for job ID {job_id}.[/yellow]")
            if verbose: console.print(JSON(latest_run_info))
            return None
    elif list_response and "runs" in list_response and not list_response["runs"]:
        console.print(f"[yellow]No runs found for job ID: {job_id}.[/yellow]")
    elif list_response :
        console.print(f"[yellow]Warning: 'runs' key missing in list response for job ID {job_id}.[/yellow]")
        if verbose: console.print(JSON(list_response))

    return None


# --- TODO: Spark Log Utilities ---
# This will be more complex, might involve:
# - Databricks CLI to download logs if API is limited.
# - Cluster log destination (DBFS, S3, Azure Blob) parsing.
# - Getting Spark UI data if available through an API (e.g. via connect/proxy or specific endpoints if they exist).
# Example placeholder:
# def get_spark_logs_for_run(host: str, token: str, cluster_id: str, run_id: int, verbose: bool = False) -> list[str] | None:
#     console.print("[yellow]Fetching Spark logs is not fully implemented yet.[/yellow]")
#     return None