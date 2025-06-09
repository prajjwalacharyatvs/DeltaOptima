from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.text import Text
from rich.markdown import Markdown
from rich.table import Table
from rich.padding import Padding
import json
from typing import Optional, Dict, Any, List

console = Console()

def display_analysis_results(report_data: Optional[Dict[str, Any]], verbose: bool = False):
    """
    Displays the structured analysis report received from the API.
    Expects report_data to be a dictionary matching the AnalysisReport Pydantic model.
    """
    if not report_data:
        console.print("[bold red]No analysis report data to display.[/bold red]")
        return

    console.print(Panel(Text("pipeLiner Analysis Report", justify="center", style="bold dodger_blue1 on white")))

    request_id = report_data.get("request_id", "N/A")
    overall_assessment = report_data.get("overall_assessment", "No overall assessment provided.")

    console.print(Padding(f"[bold]Request ID:[/bold] {request_id}", (1, 0, 0, 0)))
    console.print(Padding(f"[bold]Overall Assessment:[/bold]\n{overall_assessment}", (1, 0, 1, 0)))

    # Display Alternative Approach
    alt_approach = report_data.get("alternative_approach")
    if alt_approach and isinstance(alt_approach, dict):
        console.print(Panel(
            Text(
                f"[bold]Description:[/bold]\n{alt_approach.get('description', 'N/A')}\n\n"
                f"[bold]Suggested Overview:[/bold]\n" +
                "\n".join([f"- {step}" for step in alt_approach.get('suggested_approach_overview', [])]) +
                f"\n\n[bold]Estimated Benefits:[/bold]\n" +
                "\n".join([f"- {benefit}" for benefit in alt_approach.get('estimated_benefits', [])]),
                justify="left"
            ),
            title=f"[bold bright_magenta]:bulb: {alt_approach.get('title', 'Alternative High-Level Approach')} :bulb:[/bold bright_magenta]",
            border_style="bright_magenta",
            expand=False,
            padding=(1,2)
        ))

    # Display Code Block Suggestions
    block_suggestions = report_data.get("code_block_suggestions", [])
    if block_suggestions and isinstance(block_suggestions, list):
        console.print(Padding(Text("\n--- Code Block Specific Suggestions ---", style="bold underline green"), (1,0)))
        for i, sug in enumerate(block_suggestions):
            if not isinstance(sug, dict): continue # Skip if malformed

            title_text = Text(f"Suggestion #{i+1} for Block: '{sug.get('block_id', 'Unknown Block')}'", style="bold green")
            
            content_md = f"**Inefficiency:** {sug.get('inefficiency_summary', 'N/A')}\n\n"
            content_md += f"**Explanation:**\n{sug.get('detailed_explanation', 'N/A')}\n\n"
            if sug.get('problematic_code_snippet'):
                content_md += f"**Problematic Snippet Context:**\n```\n{sug.get('problematic_code_snippet')}\n```\n\n"
            content_md += f"**Suggested Improvement (Conceptual):**\n{sug.get('improvement_suggestion_conceptual', 'N/A')}\n"

            console.print(Panel(
                Markdown(content_md), # Use Markdown for better formatting
                title=title_text,
                border_style="green",
                expand=False,
                padding=(1,2)
            ))
    elif not block_suggestions:
         console.print("\n[italic]No specific code block suggestions were provided.[/italic]")


    # Display Common Inefficiencies
    common_inefficiencies = report_data.get("common_inefficiencies_observed", [])
    if common_inefficiencies and isinstance(common_inefficiencies, list):
        console.print(Padding(Text("\n--- Common Inefficiencies Observed ---", style="bold underline yellow"), (1,0)))
        for item in common_inefficiencies:
            console.print(f"- {item}")
    elif not common_inefficiencies:
        console.print("\n[italic]No common inefficiencies were highlighted in this report.[/italic]")

    console.print("\n" + "="*80)


def save_analysis_results(report_data: Optional[Dict[str, Any]], output_file: str, verbose: bool = False):
    """
    Saves the analysis results to a JSON file.
    """
    if not report_data:
        console.print("[bold red]No results to save.[/bold red]")
        return
    if not output_file:
        # This case should ideally be caught by Typer if output_file is mandatory and not given.
        # Or handled in main.py before calling this.
        console.print("[bold yellow]Output file path not provided, skipping save.[/bold yellow]")
        return

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        console.print(f"\n[green]Analysis report successfully saved to: {output_file}[/green]")
    except IOError as e:
        console.print(f"[bold red]Error saving results to {output_file}: {e}[/bold red]")
    except Exception as e:
        console.print(f"[bold red]An unexpected error occurred while saving results: {e}[/bold red]")   