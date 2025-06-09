import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

# Load .env file at the very beginning
load_dotenv()

from pipeTimer_src.connectors.databricks_connector import DatabricksAPIConnector
from pipeTimer_src.utils import load_config, setup_logger
from pipeTimer_src.analysis import AnalysisEngine
from pipeTimer_src.reporting import Reporter, PDFReportGenerator
from pipeTimer_src.plotting import Plotter


def analyze_notebook(notebook_path, run_id, app_config, db_host, db_token, db_cluster_log_base_path, logger):
    logger.info(f"Starting API-only analysis for notebook: {notebook_path}")
    logger.info(f"Databricks Run ID: {run_id if run_id else 'Not provided'}")
    logger.debug(f"Using general application configuration: {app_config}")

    run_api_data = None
    run_output_data = None
    connector = None

    if not run_id: # Should be caught by argparser 'required=True' now
        logger.error("A Databricks Run ID (--run-id) is required for API-based analysis.")
        return

    if db_host and db_token:
        try:
            connector = DatabricksAPIConnector(
                host=db_host,
                token=db_token,
                cluster_log_base_path=db_cluster_log_base_path,
                logger=logger
            )
            logger.info(f"Fetching Databricks run details for run_id: {run_id}...")
            run_api_data = connector.get_run_details(run_id)

            if run_api_data:
                logger.info("Successfully fetched Databricks run details.")
                logger.debug(f"Raw API Run Details: {json.dumps(run_api_data, indent=2)}")

                # If run failed, try to get output
                if run_api_data.get("result_state") and str(run_api_data.get("result_state")).upper() != "SUCCESS":
                    logger.info(f"Run {run_id} result state is {run_api_data.get('result_state')}. Fetching run output...")
                    run_output_data = connector.get_run_output(run_id)
                    if run_output_data:
                        logger.info(f"Successfully fetched run output for run_id {run_id}.")
                        logger.debug(f"Raw Run Output Data: {json.dumps(run_output_data, indent=2)}")
                    else:
                        logger.warning(f"Could not fetch run output for run_id {run_id}.")
            else:
                logger.warning(f"Could not fetch Databricks run details for run_id: {run_id}.")
                return
        except ValueError as e:
            logger.error(f"Failed to initialize or use Databricks Connector: {e}")
            return
        except Exception as e:
            logger.error(f"An unexpected error occurred during Databricks API interaction: {e}", exc_info=True)
            return
    else:
        logger.error("Databricks host and token are required to fetch run details.")
        return
    
    if not run_api_data:
        logger.error("No data fetched from Databricks API. Aborting analysis.")
        return

    engine = AnalysisEngine(logger=logger)
    # Use the renamed method for processing individual run details
    processed_data = engine.process_run_details_for_report(run_api_data, run_output_data)


    if not processed_data:
        logger.error("Failed to process API data with AnalysisEngine.")
        return

    reporter = Reporter(logger=logger)
    text_summary = reporter.generate_text_summary(processed_data)
    print("\n" + text_summary)

    logger.info(f"\nAPI-only analysis complete for: {notebook_path}")


def generate_job_report(job_id, days, output_bar_chart_path_arg, output_pie_chart_path_arg, 
                        generate_pdf_filename, app_config, db_host, db_token, logger):
    logger.info(f"Generating report for Job ID: {job_id}, History: last {days} days.")

    if not db_host or not db_token:
        logger.error("Databricks host and token are required. Provide via CLI or .env.")
        return

    # Initialize paths for charts
    bar_chart_final_path_for_plotter = None
    pie_chart_final_path_for_plotter = None
    
    # These will store the actual paths where images are saved, for PDF use
    actual_bar_chart_saved_path = None
    actual_pie_chart_saved_path = None
    
    temp_files_for_pdf_cleanup = []

    ts_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Determine filenames for charts
    if output_bar_chart_path_arg:
        bar_chart_final_path_for_plotter = output_bar_chart_path_arg
    elif generate_pdf_filename: # If PDF, chart is temp unless user also specifies output-bar-chart
        bar_chart_final_path_for_plotter = f"temp_job_{job_id}_duration_history_{ts_suffix}.png"
    else: # Default name if no PDF and no specific output given for bar chart
        bar_chart_final_path_for_plotter = f"pipetimer_job_{job_id}_durations_{days}d_{ts_suffix}.png"

    if output_pie_chart_path_arg:
        pie_chart_final_path_for_plotter = output_pie_chart_path_arg
    elif generate_pdf_filename:
        pie_chart_final_path_for_plotter = f"temp_job_{job_id}_status_pie_{ts_suffix}.png"
    else:
        pie_chart_final_path_for_plotter = f"pipetimer_job_{job_id}_status_pie_{days}d_{ts_suffix}.png"

    processed_job_details = None
    run_history_analysis_data = None

    try:
        connector = DatabricksAPIConnector(host=db_host, token=db_token, logger=logger)
        engine = AnalysisEngine(logger=logger)
        plotter = Plotter(logger=logger)

        logger.info(f"Fetching static details for Job ID: {job_id}...")
        job_api_data = connector.get_job_details(job_id)
        if job_api_data:
            processed_job_details = engine.process_job_details(job_api_data)
        else:
            logger.warning(f"Could not fetch static details for Job ID {job_id}.")

        logger.info(f"Fetching run history for Job ID: {job_id}...")
        job_runs_raw = connector.list_job_runs(job_id=job_id, days_history=days)

        if job_runs_raw:
            run_history_analysis_data = engine.analyze_run_history(job_runs_raw)
            
            plot_data_for_bar = engine.prepare_runs_for_plotting(job_runs_raw)
            if plot_data_for_bar:
                actual_bar_chart_saved_path = plotter.plot_job_run_durations(plot_data_for_bar, job_id, bar_chart_final_path_for_plotter)
                if actual_bar_chart_saved_path:
                    if generate_pdf_filename and (not output_bar_chart_path_arg): 
                        temp_files_for_pdf_cleanup.append(actual_bar_chart_saved_path)
                    logger.info(f"Bar chart saved to: {os.path.abspath(actual_bar_chart_saved_path)}")
                    if not generate_pdf_filename: print(f"\nBar chart saved to: {os.path.abspath(actual_bar_chart_saved_path)}")
                else: logger.error("Failed to generate bar chart.")

            if run_history_analysis_data and run_history_analysis_data.get("status_counts"):
                actual_pie_chart_saved_path = plotter.plot_run_status_pie_chart(run_history_analysis_data["status_counts"], job_id, pie_chart_final_path_for_plotter)
                if actual_pie_chart_saved_path:
                    if generate_pdf_filename and (not output_pie_chart_path_arg):
                        temp_files_for_pdf_cleanup.append(actual_pie_chart_saved_path)
                    logger.info(f"Pie chart saved to: {os.path.abspath(actual_pie_chart_saved_path)}")
                    if not generate_pdf_filename: print(f"\nPie chart saved to: {os.path.abspath(actual_pie_chart_saved_path)}")
                else: logger.error("Failed to generate pie chart.")
        else:
            logger.warning(f"No completed runs found for Job ID {job_id} in the last {days} days to generate plots/history analysis.")

        if generate_pdf_filename:
            if not processed_job_details and not run_history_analysis_data and not actual_bar_chart_saved_path and not actual_pie_chart_saved_path:
                logger.error("No data or charts available to generate PDF report.")
                return

            pdf_reporter = PDFReportGenerator(filename=generate_pdf_filename, logger=logger)
            final_pdf_path = pdf_reporter.generate_report(
                job_id=job_id,
                processed_job_details=processed_job_details,
                run_history_analysis=run_history_analysis_data,
                bar_chart_path=actual_bar_chart_saved_path, 
                pie_chart_path=actual_pie_chart_saved_path  
            )
            if final_pdf_path:
                logger.info(f"PDF report generated: {os.path.abspath(final_pdf_path)}")
                print(f"\nPDF report generated: {os.path.abspath(final_pdf_path)}")
            else:
                logger.error(f"Failed to generate PDF report for Job ID {job_id}.")

    except ValueError as e: 
        logger.error(f"Configuration or input error: {e}")
    except Exception as e:
        logger.error(f"An error occurred while generating job report: {e}", exc_info=True)
    finally:
        if generate_pdf_filename: 
            for temp_file in temp_files_for_pdf_cleanup:
                if temp_file and os.path.exists(temp_file): # temp_file is already an absolute or relative path
                    try:
                        os.remove(temp_file)
                        logger.info(f"Cleaned up temporary image: {temp_file}")
                    except Exception as e_clean:
                        logger.warning(f"Could not clean up temp file {temp_file}: {e_clean}")


def main():
    parser = argparse.ArgumentParser(
        description="pipeTimer: Databricks Pipeline Efficiency Analyzer.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-c', '--config', default=os.path.join('config', 'settings.json'),
        help='Path to the general configuration file.'
    )
    db_group = parser.add_argument_group('Databricks Connection Options')
    db_group.add_argument('--databricks-host', help='Databricks workspace host URL (overrides .env).')
    db_group.add_argument('--databricks-token', help='Databricks API token (overrides .env).')
    db_group.add_argument('--databricks-cluster-log-base-path', help='DBFS base path for cluster logs (for future event log download).')

    subparsers = parser.add_subparsers(dest="command", help="Available commands", required=True)

    # 'analyze' command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze a specific Databricks notebook run.")
    analyze_parser.add_argument("--notebook-path", required=True, help="Contextual Databricks notebook path associated with the run.")
    analyze_parser.add_argument("--run-id", required=True, help="Specific Databricks job run ID to analyze.")
    # analyze_parser.add_argument(
    #     "--event-log-path", # Kept commented out as logs are not used currently
    #     help="[DEFERRED] Local path to the Spark event log file."
    # )

    # 'report-job' command (previously 'history')
    report_parser = subparsers.add_parser("report-job", help="Generate a report with history for a Databricks Job.")
    report_parser.add_argument("--job-id", type=int, required=True, help="Databricks Job ID.")
    report_parser.add_argument("--days", type=int, default=30, help="Number of past days of run history.")
    report_parser.add_argument("--output-bar-chart", help="Optional: Filename to save the duration bar chart (e.g., durations.png).")
    report_parser.add_argument("--output-pie-chart", help="Optional: Filename to save the status pie chart (e.g., statuses.png).")
    report_parser.add_argument("--generate-pdf", metavar="PDF_FILENAME",
                               help="Generate a PDF report with the specified filename (e.g., job_report.pdf). Includes charts and job details.")
    
    args = parser.parse_args()
    
    config_path_arg = args.config
    resolved_config_path = config_path_arg # Default if absolute or not found by relative logic
    if not os.path.isabs(config_path_arg):
        potential_path_cwd = os.path.join(os.getcwd(), config_path_arg)
        potential_path_scriptdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_path_arg)
        if os.path.exists(potential_path_cwd):
            resolved_config_path = potential_path_cwd
        elif os.path.exists(potential_path_scriptdir):
            resolved_config_path = potential_path_scriptdir
        # If neither exists, load_config will handle the FileNotFoundError with the original path

    app_config = load_config(config_path=resolved_config_path)
    main_logger = setup_logger(name="pipeTimerCLI", level_str=app_config.get("log_level", "INFO").upper())

    db_host = args.databricks_host or os.environ.get("DATABRICKS_HOST")
    db_token = args.databricks_token or os.environ.get("DATABRICKS_TOKEN")
    db_cluster_log_base_path = args.databricks_cluster_log_base_path or os.environ.get("DATABRICKS_CLUSTER_LOG_BASE_PATH")

    if args.command == "analyze":
        main_logger.info(f"Executing 'analyze' command for notebook: {args.notebook_path}, Run ID: {args.run_id}")
        if not db_host or not db_token:
            main_logger.error("Databricks host and token are required for the 'analyze' command.")
            sys.exit(1)
        analyze_notebook(
            notebook_path=args.notebook_path, 
            run_id=args.run_id, 
            app_config=app_config, 
            db_host=db_host, 
            db_token=db_token, 
            db_cluster_log_base_path=db_cluster_log_base_path, 
            logger=main_logger
        )
    
    elif args.command == "report-job":
        main_logger.info(f"Executing 'report-job' command for Job ID: {args.job_id}, Days: {args.days}")
        if not db_host or not db_token:
            main_logger.error("Databricks host and token are required for the 'report-job' command.")
            sys.exit(1)
        
        generate_job_report(
            job_id=args.job_id,
            days=args.days,
            output_bar_chart_path_arg=args.output_bar_chart,
            output_pie_chart_path_arg=args.output_pie_chart,
            generate_pdf_filename=args.generate_pdf,
            app_config=app_config,
            db_host=db_host,
            db_token=db_token,
            logger=main_logger
        )
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()