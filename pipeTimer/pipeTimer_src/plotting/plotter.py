import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt_module
import statistics
from ..utils import setup_logger

class Plotter:
    def __init__(self, logger=None):
        self.logger = logger or setup_logger(name="Plotter")

    def plot_job_run_durations(self, prepared_plot_data, job_id, output_filename="job_run_durations.png"):
        # ... (existing method - light theme, indigo bars, red median line - no change from previous) ...
        if not prepared_plot_data:
            self.logger.warning(f"No processed job runs provided for plotting for job ID {job_id}.")
            return None
        try:
            timestamps = [run['start_time_dt'] for run in prepared_plot_data]
            durations_minutes = [run['duration_minutes'] for run in prepared_plot_data]
            if not timestamps or not durations_minutes:
                self.logger.warning(f"No valid data points to plot for job ID {job_id} after preparation.")
                return None
            fig, ax = plt.subplots(figsize=(14, 8))
            fig.patch.set_facecolor('white') 
            ax.set_facecolor('white')
            bar_width_days = 0.8 
            if len(timestamps) > 1:
                time_diffs_seconds = [(timestamps[i] - timestamps[i-1]).total_seconds() for i in range(1, len(timestamps))]
                if time_diffs_seconds: 
                    median_diff_days = statistics.median(time_diffs_seconds) / (24 * 3600.0)
                    if median_diff_days > 0: 
                         bar_width_days = max(0.05, median_diff_days * 0.6) 
            actual_bar_width = dt_module.timedelta(days=bar_width_days)
            ax.bar(timestamps, durations_minutes, width=actual_bar_width, color='indigo', 
                   align='center', label='Execution Duration', edgecolor='black', linewidth=0.7)
            if len(durations_minutes) > 0:
                median_duration = statistics.median(durations_minutes)
                ax.axhline(y=median_duration, color='red', linestyle='--', linewidth=2,
                           label=f'Median: {median_duration:.2f} min')
            title_color = 'black'; label_color = 'dimgray'; tick_color = 'dimgray'
            ax.set_title(f"Execution Durations for Databricks Job ID: {job_id}", fontweight='bold', fontsize=16, color=title_color)
            ax.set_xlabel("Run Start Time (UTC)", fontweight='bold', fontsize=12, color=label_color)
            ax.set_ylabel("Execution Duration (minutes)", fontweight='bold', fontsize=12, color=label_color)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
            fig.autofmt_xdate(rotation=30, ha="right") 
            ax.tick_params(axis='x', colors=tick_color, labelsize=10)
            ax.tick_params(axis='y', colors=tick_color, labelsize=10)
            ax.spines['top'].set_color('lightgray'); ax.spines['right'].set_color('lightgray')
            ax.spines['left'].set_color('darkgray'); ax.spines['bottom'].set_color('darkgray')
            ax.grid(True, linestyle=':', alpha=0.6, axis='y', color='lightgray')
            ax.set_axisbelow(True)
            if len(durations_minutes) > 0:
                legend = ax.legend(fontsize=10, frameon=True, facecolor='white', edgecolor='lightgray')
                for text in legend.get_texts(): text.set_color(label_color)
            plt.subplots_adjust(left=0.08, right=0.95, bottom=0.25, top=0.90)
            plt.savefig(output_filename, facecolor=fig.get_facecolor(), dpi=100)
            plt.close(fig)
            self.logger.info(f"Successfully saved job run duration bar plot (light theme) to: {output_filename}")
            return output_filename
        except Exception as e:
            self.logger.error(f"Failed to generate or save plot for job ID {job_id}: {e}", exc_info=True)
            return None
        finally:
            plt.style.use('default')


    def plot_run_status_pie_chart(self, status_counts, job_id, output_filename="job_status_pie.png"):
        """
        Plots a pie chart of job run statuses.
        Args:
            status_counts (dict): A dictionary with status names as keys and counts as values.
                                 e.g., {'SUCCESS': 10, 'FAILED': 2, 'CANCELED': 1}
            job_id (str/int): The Job ID for titling the plot.
            output_filename (str): The filename to save the plot.
        Returns:
            str: Path to the saved plot file, or None if plotting failed.
        """
        if not status_counts:
            self.logger.warning(f"No run status counts provided for pie chart for job ID {job_id}.")
            return None

        # Define consistent colors for statuses - focus on indigo, red, white/grays
        # SUCCESS: indigo-like, FAILED: red, CANCELED: gray, others: lightgray/blue variants
        status_colors = {
            'SUCCESS': 'mediumpurple', # Indigo family
            'FAILED': 'crimson',       # Red
            'CANCELED': 'silver',
            'TIMEDOUT': 'lightcoral', # Reddish
            'INTERNAL_ERROR': 'indianred', # Reddish
            'UNKNOWN': 'lightgray'
        }
        
        labels = list(status_counts.keys())
        sizes = list(status_counts.values())
        # Get colors for the actual labels present, defaulting for unknown ones
        colors = [status_colors.get(label.upper(), 'lightgray') for label in labels]

        try:
            # Ensure a light theme for this plot too
            # plt.style.use('default') # Reset if a global style was set before

            fig, ax = plt.subplots(figsize=(8, 8))
            fig.patch.set_facecolor('white')
            
            # Explode the 'FAILED' slice a bit if it exists
            explode_values = [0.1 if label.upper() == 'FAILED' else 0 for label in labels]

            ax.pie(sizes, explode=explode_values, labels=labels, colors=colors,
                   autopct=lambda p: '{:.1f}%\n({:.0f})'.format(p, p * sum(sizes) / 100), # Shows percentage and count
                   shadow=False, startangle=90, pctdistance=0.8,
                   textprops={'color': 'black', 'fontweight': 'normal'}) # Ensure text is readable
            
            ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
            plt.title(f"Run Status Distribution for Job ID: {job_id}", fontweight='bold', fontsize=14, color='black')
            
            plt.savefig(output_filename, facecolor=fig.get_facecolor(), dpi=100)
            plt.close(fig)
            
            self.logger.info(f"Successfully saved run status pie chart to: {output_filename}")
            return output_filename
        except Exception as e:
            self.logger.error(f"Failed to generate or save pie chart for job ID {job_id}: {e}", exc_info=True)
            return None
        finally:
            plt.style.use('default')


if __name__ == '__main__':
    test_logger = setup_logger(name="PlotterTest", level_str="DEBUG")
    plotter = Plotter(logger=test_logger)

    # ... (existing test for plot_job_run_durations) ...
    sample_prepared_runs_for_bar = [
        {'start_time_dt': dt_module.datetime.now(dt_module.timezone.utc) - dt_module.timedelta(days=5), 'duration_minutes': 5.0},
        {'start_time_dt': dt_module.datetime.now(dt_module.timezone.utc) - dt_module.timedelta(days=3), 'duration_minutes': 4.0},
    ]
    plotter.plot_job_run_durations(sample_prepared_runs_for_bar, job_id="LIGHT_THEME_JOB", output_filename="sample_job_history_light.png")
    print("Bar chart example generated.")

    # Test plot_run_status_pie_chart
    sample_status_counts = {'SUCCESS': 20, 'FAILED': 3, 'CANCELED': 2, 'TIMEDOUT': 1}
    pie_output_file = plotter.plot_run_status_pie_chart(sample_status_counts, job_id="PIE_CHART_JOB", output_filename="sample_status_pie.png")
    if pie_output_file:
        print(f"Sample pie chart saved by Plotter to: {pie_output_file}")
    else:
        print("Failed to save sample pie chart.")

    empty_status_counts = {}
    plotter.plot_run_status_pie_chart(empty_status_counts, job_id="EMPTY_PIE_JOB", output_filename="empty_status_pie.png")
    print("Pie chart test with no data completed (should warn and return None).")