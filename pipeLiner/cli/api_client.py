# pipeLiner/cli/api_client.py

import requests
import json # Though Pydantic model.model_dump() gives a dict, requests can send it as json
from rich.console import Console
from typing import Optional, Dict, Any

console = Console()

# Default URL for the locally running FastAPI app
# This could be made configurable later (e.g., via .env or a config command)
DEFAULT_API_URL = "http://localhost:8000/api/v1/analyze/"

def call_analysis_api(
    payload: Dict[str, Any], # Expects a dictionary (from Pydantic's model_dump())
    api_url: str = DEFAULT_API_URL,
    verbose: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Sends the analysis payload to the pipeLiner FastAPI service.

    Args:
        payload: The analysis request data as a dictionary.
        api_url: The URL of the FastAPI endpoint.
        verbose: If True, prints more detailed logs.

    Returns:
        The JSON response from the API as a dictionary, or None if an error occurs.
    """
    if verbose:
        console.print(f"[italic blue]Attempting to send analysis request to: {api_url}[/italic blue]")
        # For brevity, don't print the full payload here again if already printed in main.py
        # but you could add: console.print(f"Payload being sent: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(api_url, json=payload, timeout=60) # timeout in seconds
        response.raise_for_status()  # Raise an HTTPError for bad responses (4XX or 5XX)
        
        response_data = response.json()
        if verbose:
            console.print(f"[green]Successfully received response from API for request ID: {payload.get('request_id', 'N/A')}[/green]")
        return response_data

    except requests.exceptions.HTTPError as errh:
        console.print(f"[bold red]API Client - Http Error calling {api_url}:[/bold red] {errh}")
        if errh.response is not None:
            try:
                error_details = errh.response.json()
                console.print("[bold red]API Error Details:[/bold red]")
                from rich.json import JSON # Local import for this specific print
                console.print(JSON(json.dumps(error_details)))
            except ValueError:
                console.print(f"[bold red]API Error Response (non-JSON):[/bold red] {errh.response.text[:500]}")
    except requests.exceptions.ConnectionError as errc:
        console.print(f"[bold red]API Client - Error Connecting to {api_url}:[/bold red] {errc}")
        console.print("[bold yellow]Hint: Is the FastAPI server (uvicorn fast_api_app.main:app --reload) running?[/bold yellow]")
    except requests.exceptions.Timeout as errt:
        console.print(f"[bold red]API Client - Timeout Error calling {api_url}:[/bold red] {errt}")
    except requests.exceptions.RequestException as err:
        console.print(f"[bold red]API Client - Request Exception calling {api_url}:[/bold red] {err}")
    except ValueError as json_err: # Includes JSONDecodeError if response isn't valid JSON
        console.print(f"[bold red]API Client - JSON Decode Error from {api_url}:[/bold red] {json_err}")
        if 'response' in locals() and response is not None: # Check if response object exists
             console.print(f"Raw response text (first 200 chars): {response.text[:200] if response.text else '[EMPTY RESPONSE]'}")


    return None