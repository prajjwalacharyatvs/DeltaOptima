import typer
from rich.console import Console
from getpass import getpass

console = Console()

TOKEN_KEY = "DATABRICKS_TOKEN"
HOST_KEY = "DATABRICKS_HOST"
HTTP_PATH_KEY = "DATABRICKS_HTTP_PATH" 

def get_databricks_credentials(
    current_token: str | None = None,
    current_host: str | None = None,
    current_http_path: str | None = None
) -> dict:
    """
    Prompts the user for Databricks credentials if they are not already provided.
    Returns a dictionary with the token, host, and http_path.
    """
    creds = {
        "token": current_token,
        "host": current_host,
        "http_path": current_http_path,
    }

    if not creds["host"]:
        console.print("\n[bold yellow]Databricks Hostname[/bold yellow] (e.g., [italic]adb-xxxx.xx.azuredatabricks.net[/italic] or [italic]your-company.cloud.databricks.com[/italic]):")
        creds["host"] = typer.prompt("Enter Hostname")
        if not creds["host"]:
            console.print("[bold red]Error:[/bold red] Hostname is required.")
            raise typer.Exit(code=1)
        # Basic validation: should not start with https://
        if creds["host"].startswith("https://"):
            creds["host"] = creds["host"].replace("https://", "")
            console.print(f"[italic dim]Using hostname: {creds['host']}[/italic dim]")


    if not creds["token"]:
        console.print("\n[bold yellow]Databricks API Token[/bold yellow] (starts with 'dapi...'):")
        # Use getpass to hide token input for security
        creds["token"] = getpass("Enter Token: ")
        if not creds["token"]:
            console.print("[bold red]Error:[/bold red] Token is required.")
            raise typer.Exit(code=1)

    if not creds["http_path"]:
        console.print("\n[bold yellow]Databricks SQL Warehouse HTTP Path or Cluster ID[/bold yellow]:")
        console.print("For SQL Warehouses (recommended for SQL analysis): "
                      "e.g., [italic]/sql/1.0/warehouses/xxxxxxxxxxxxxxxx[/italic]")
        console.print("For All-Purpose Clusters (for notebook command execution context if not using SQL Warehouse): "
                      "e.g., [italic]0123-456789-abcdefgh[/italic] (Cluster ID)")
        creds["http_path"] = typer.prompt("Enter HTTP Path or Cluster ID (can be optional for some operations, press Enter to skip if not needed now)")
        if not creds["http_path"]: # If user presses enter, http_path will be empty string
            creds["http_path"] = None # Store as None if empty
            console.print("[italic dim]No HTTP Path / Cluster ID provided.[/italic dim]")


    console.print("\n[bold green]Credentials received.[/bold green] (Token is hidden for security)")
    return creds
