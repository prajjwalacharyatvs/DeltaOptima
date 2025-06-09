# pipeTimer_src/reporting/pdf_reporter.py
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle, KeepInFrame
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.pdfbase.ttfonts import TTFont # For bold fonts if needed explicitly
from reportlab.pdfbase import pdfmetrics
import os
from ..utils import setup_logger

class PDFReportGenerator:
    def __init__(self, filename="pipeTimer_report.pdf", logger=None):
        self.filename = filename
        self.logger = logger or setup_logger(name="PDFReporter")
        self.styles = getSampleStyleSheet()
        self.story = []

        # --- Custom Style Definitions ---
        self.styles.add(ParagraphStyle(name='ReportTitle', parent=self.styles['h1'], 
                                       fontSize=18, alignment=TA_CENTER, spaceAfter=12,
                                       textColor=colors.HexColor('#000040'))) # Darker Indigo for title
        
        self.styles.add(ParagraphStyle(name='JobTitle', parent=self.styles['h2'], 
                                       fontSize=14, alignment=TA_CENTER, spaceAfter=10, 
                                       textColor=colors.HexColor('#202060'))) # Slightly lighter Indigo

        self.styles.add(ParagraphStyle(name='SectionTitle', parent=self.styles['h2'], 
                                       fontSize=14, leading=16, spaceBefore=18, spaceAfter=6,
                                       alignment=TA_LEFT, textColor=colors.HexColor('#000066'))) # Indigo

        self.styles.add(ParagraphStyle(name='SubSectionTitle', parent=self.styles['h3'],
                                       fontSize=12, leading=14, spaceBefore=10, spaceAfter=4,
                                       alignment=TA_LEFT, textColor=colors.HexColor('#111111'))) # Dark gray

        self.styles.add(ParagraphStyle(name='DetailKey', parent=self.styles['Normal'],
                                       fontName='Helvetica-Bold', # Use a bold font
                                       alignment=TA_LEFT, spaceBefore=3))
        
        self.styles.add(ParagraphStyle(name='DetailValue', parent=self.styles['Normal'],
                                       alignment=TA_LEFT, leftIndent=15, spaceBefore=3, spaceAfter=3))
        
        # Define the 'Bold' style explicitly based on 'Normal'
        self.styles.add(ParagraphStyle(name='Bold', parent=self.styles['Normal'],
                                       fontName='Helvetica-Bold')) # Standard bold font

        self.styles.add(ParagraphStyle(name='SmallNormal', parent=self.styles['Normal'], fontSize=8))
        self.styles.add(ParagraphStyle(name='CenteredSmall', parent=self.styles['SmallNormal'], alignment=TA_CENTER))
        self.styles.add(ParagraphStyle(name='ItalicSmall', parent=self.styles['SmallNormal'], fontName='Helvetica-Oblique'))


    def _add_title(self, job_id, job_name):
        title_text = f"pipeTimer Analysis Report"
        self.story.append(Paragraph(title_text, self.styles['ReportTitle']))
        sub_title = f"Databricks Job ID: {job_id}{f' ({job_name})' if job_name and job_name != 'N/A' else ''}"
        self.story.append(Paragraph(sub_title, self.styles['JobTitle'])) # Using new JobTitle style
        self.story.append(Spacer(1, 0.2 * inch))

    def _add_key_value_pair(self, key_text, value_text_or_list, key_style='DetailKey', value_style='DetailValue'):
        self.story.append(Paragraph(key_text, self.styles[key_style]))
        if isinstance(value_text_or_list, list):
            if not value_text_or_list:
                self.story.append(Paragraph("N/A", self.styles[value_style]))
            for item in value_text_or_list:
                self.story.append(Paragraph(str(item), self.styles[value_style]))
        elif isinstance(value_text_or_list, dict): # For tags
            if not value_text_or_list: self.story.append(Paragraph("N/A", self.styles[value_style]))
            for k, v_item in value_text_or_list.items(): # Changed v to v_item
                 self.story.append(Paragraph(f"- {k}: {v_item}", self.styles[value_style]))
        else:
            self.story.append(Paragraph(str(value_text_or_list), self.styles[value_style]))

    def _add_job_configuration_section(self, processed_job_details):
        if not processed_job_details: return
        self.story.append(Paragraph("Job Configuration Details", self.styles['SectionTitle']))
        
        self._add_key_value_pair("Job Name:", processed_job_details.get('name', 'N/A'))
        self._add_key_value_pair("Creator:", processed_job_details.get('creator_user_name', 'N/A'))
        self._add_key_value_pair("Created (UTC):", processed_job_details.get('created_time_utc', 'N/A'))
        schedule_str = f"{processed_job_details.get('schedule_cron', 'Not Scheduled')}"
        if processed_job_details.get('schedule_timezone', 'N/A') != 'N/A' and processed_job_details.get('schedule_cron', 'N/A') != 'Not Scheduled':
            schedule_str += f" ({processed_job_details.get('schedule_timezone')})"
        self._add_key_value_pair("Schedule:", schedule_str)
        
        self.story.append(Paragraph("Tasks:", self.styles['DetailKey']))
        for task_summary in processed_job_details.get('tasks_summary', ['N/A']):
            self.story.append(Paragraph(f"- {task_summary}", self.styles['DetailValue']))

        self._add_key_value_pair("Default Cluster Info:", processed_job_details.get('default_cluster_info', 'N/A'))
        self._add_key_value_pair("Max Concurrent Runs:", processed_job_details.get('max_concurrent_runs', 'N/A'))
        self._add_key_value_pair("Timeout (seconds):", processed_job_details.get('timeout_seconds', 'N/A'))
        self._add_key_value_pair("Retry on Timeout:", "Yes" if processed_job_details.get('retry_on_timeout') else "No")
        
        self.story.append(Paragraph("Email Notifications:", self.styles['DetailKey']))
        for notification_summary in processed_job_details.get('notification_summary', ['None configured']):
            self.story.append(Paragraph(f"- {notification_summary}", self.styles['DetailValue']))

        self.story.append(Paragraph("Tags:", self.styles['DetailKey'])) # Handle dict in _add_key_value_pair
        self._add_key_value_pair("", processed_job_details.get('tags', {})) # Pass empty key, let value handler do dict

        self.story.append(Paragraph("Libraries:", self.styles['DetailKey']))
        libs = processed_job_details.get('libraries', [])
        if libs:
            for lib in libs:
                lib_type = list(lib.keys())[0]
                lib_val = lib[lib_type]
                # Handle different library structures (e.g. pypi.package, maven.coordinates)
                if isinstance(lib_val, dict): 
                    lib_display = f"{lib_type}: {', '.join(f'{k}={v}' for k,v in lib_val.items())}"
                else:
                    lib_display = f"{lib_type}: {lib_val}"
                self.story.append(Paragraph(f"- {lib_display}", self.styles['DetailValue']))
        else:
            self.story.append(Paragraph("None", self.styles['DetailValue']))

        self.story.append(Spacer(1, 0.2 * inch))

    def _add_run_history_summary_section(self, history_analysis):
        if not history_analysis: return
        self.story.append(Paragraph("Run History Summary", self.styles['SectionTitle']))
        
        self._add_key_value_pair("Period Analyzed (UTC):", f"{history_analysis.get('period_start_utc')} to {history_analysis.get('period_end_utc')}")
        self._add_key_value_pair("Total Runs in Period:", history_analysis.get('total_runs_analyzed', 'N/A'))
        self._add_key_value_pair("Success Rate:", history_analysis.get('success_rate_percent', 'N/A'))

        self.story.append(Paragraph("Execution Duration Statistics (for completed runs):", self.styles['SubSectionTitle']))
        stats_str = history_analysis.get('duration_stats_str', {})
        self._add_key_value_pair("Min Duration:", stats_str.get('min', 'N/A'))
        self._add_key_value_pair("Max Duration:", stats_str.get('max', 'N/A'))
        self._add_key_value_pair("Average Duration:", stats_str.get('mean', 'N/A'))
        self._add_key_value_pair("Median Duration:", stats_str.get('median', 'N/A'))
        self._add_key_value_pair("Std Dev Duration:", stats_str.get('std_dev', 'N/A'))
        self.story.append(Spacer(1, 0.2 * inch))

    def _add_notable_runs_section(self, history_analysis):
        if not history_analysis or not history_analysis.get('notable_runs'): return
        
        notable = history_analysis['notable_runs']
        
        # Use the defined 'Bold' style for table headers
        header_style = self.styles['Bold'] 
        # Use 'SmallNormal' for table cell content to fit more
        cell_style = self.styles['SmallNormal'] 

        def create_runs_table(runs_list, title_text): # Renamed title to title_text
            if not runs_list: return None
            
            # Table Title as a Paragraph, part of the story, not the table data directly
            # self.story.append(Paragraph(title_text, self.styles['SubSectionTitle'])) # Add title before table

            data = [[Paragraph(h, header_style) for h in ["Run ID", "Start Time (UTC)", "Duration", "Result"]]]
            for run in runs_list:
                data.append([
                    Paragraph(str(run.get("run_id")), cell_style),
                    Paragraph(str(run.get("start_time_utc")), cell_style),
                    Paragraph(str(run.get("duration_str")), cell_style),
                    Paragraph(str(run.get("result_state")), cell_style)
                ])
            
            table = Table(data, colWidths=[1.2*inch, 1.8*inch, 1.5*inch, 1.5*inch]) # Adjusted colWidths
            table.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#D0D0FF')), # Light Indigo background for header
                ('TEXTCOLOR', (0,0), (-1,0), colors.black),
                ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), # Ensure header font is bold
                ('FONTSIZE', (0,0), (-1,-1), 8), # Set uniform font size for table
                ('LEFTPADDING', (0,0), (-1,-1), 3),
                ('RIGHTPADDING', (0,0), (-1,-1), 3),
                ('TOPPADDING', (0,0), (-1,-1), 2),
                ('BOTTOMPADDING', (0,0), (-1,-1), 2),
            ]))
            return table

        # Add SubSectionTitle before calling create_runs_table
        if notable.get("longest"):
            self.story.append(Paragraph("Longest Duration Runs (Top 3)", self.styles['SubSectionTitle']))
            longest_table = create_runs_table(notable.get("longest"), "Longest Duration Runs (Top 3)")
            if longest_table: self.story.append(longest_table); self.story.append(Spacer(1, 0.1*inch))
        
        if notable.get("shortest"):
            self.story.append(Paragraph("Shortest Duration Runs (Top 3)", self.styles['SubSectionTitle']))
            shortest_table = create_runs_table(notable.get("shortest"), "Shortest Duration Runs (Top 3)")
            if shortest_table: self.story.append(shortest_table); self.story.append(Spacer(1, 0.1*inch))

        if notable.get("recent_failures"):
            self.story.append(Paragraph("Recent Non-Successful Runs (Last 3)", self.styles['SubSectionTitle']))
            failed_table = create_runs_table(notable.get("recent_failures"), "Recent Non-Successful Runs (Last 3)")
            if failed_table: self.story.append(failed_table); self.story.append(Spacer(1, 0.2*inch))


    def _add_image_to_story(self, image_path, caption_text, width=6.5*inch):
        # ... (This method remains largely the same as before, ensure image scaling is robust) ...
        if not image_path or not os.path.exists(image_path):
            self.logger.warning(f"Image not found at {image_path if image_path else 'N/A'}, skipping.")
            self.story.append(Paragraph(f"(Chart: {caption_text} - Image not found)", self.styles['ItalicSmall'])) # Use defined ItalicSmall
            return

        self.story.append(Paragraph(caption_text, self.styles['SubSectionTitle']))
        try:
            img = Image(image_path) 
            img_width, img_height = img.imageWidth, img.imageHeight
            aspect = img_height / float(img_width) if img_width > 0 else 1
            display_width = width
            display_height = display_width * aspect
            
            max_img_height = 4.5 * inch 
            if display_height > max_img_height:
                display_height = max_img_height
                display_width = display_height / aspect if aspect > 0 else display_width # Avoid division by zero

            img.drawWidth = display_width
            img.drawHeight = display_height
            
            img_table_data = [[img]]
            img_table = Table(img_table_data, colWidths=[display_width]) # Table takes list of list of flowables
            img_table.setStyle(TableStyle([('ALIGN', (0,0), (0,0), 'CENTER')]))
            
            # KeepInFrame might be too restrictive if images are large; direct append might be better
            # For now, keeping it to try and manage page breaks.
            frame_height = display_height + 0.5 * inch # Approximate height for image + caption space
            frame = KeepInFrame(width + 0.2*inch, frame_height, [img_table], hAlign='CENTER') 
            self.story.append(frame)

        except Exception as e:
            self.logger.error(f"Could not add image {image_path} to PDF: {e}")
            self.story.append(Paragraph(f"(Error adding chart: {caption_text})", self.styles['ItalicSmall']))
        self.story.append(Spacer(1, 0.1 * inch))

    def generate_report(self, job_id, processed_job_details, run_history_analysis, 
                        bar_chart_path=None, pie_chart_path=None):
        self.logger.info(f"Generating PDF report for job ID: {job_id}")
        doc = SimpleDocTemplate(self.filename,
                                leftMargin=0.75*inch, rightMargin=0.75*inch, # Adjusted margins
                                topMargin=0.75*inch, bottomMargin=0.75*inch)
        
        self.story = [] 
        self._add_title(job_id, processed_job_details.get("name") if processed_job_details else "N/A")
        
        if processed_job_details:
            self._add_job_configuration_section(processed_job_details)
        
        if run_history_analysis:
            self.story.append(PageBreak())
            self._add_run_history_summary_section(run_history_analysis)
            self._add_notable_runs_section(run_history_analysis)

        if bar_chart_path:
            # Check if enough space on current page or break
            self.story.append(PageBreak()) # Force charts to new pages for now for simplicity
            self._add_image_to_story(bar_chart_path, "Job Run Duration History (Bar Chart)")
        
        if pie_chart_path:
            self.story.append(PageBreak())
            self._add_image_to_story(pie_chart_path, "Completed Run Status Distribution")
            
        try:
            doc.build(self.story)
            self.logger.info(f"PDF report successfully generated: {self.filename}")
            return self.filename
        except Exception as e:
            self.logger.error(f"Failed to build PDF document: {e}", exc_info=True)
            return None

if __name__ == '__main__':
    # ... (The __main__ block from the previous version can be used here, 
    # ensure it passes the sample_history_analysis to generate_report) ...
    dummy_logger = setup_logger("PDFReportTest", "DEBUG")
    pdf_gen = PDFReportGenerator(filename="test_job_report_enhanced_v2.pdf", logger=dummy_logger)

    sample_job_details = {
        "job_id": "TEST_JOB_123", "name": "My Sample ETL Job", 
        "creator_user_name": "testuser@example.com", "created_time_utc": "2023-01-01 10:00:00 UTC",
        "schedule_cron": "0 0 * * * ?", "schedule_timezone": "America/New_York",
        "tasks_summary": ["Task_1: Notebook /Shared/ETL/Ingest", "Task_2: Notebook /Shared/ETL/Transform"],
        "default_cluster_info": "New Job Cluster: 10.4.x-scala2.12, i3.xlarge, Workers: 5",
        "max_concurrent_runs": 1, "timeout_seconds": 7200, "retry_on_timeout": True,
        "notification_summary": ["On Failure: admin@example.com", "On Success: user@example.com"], 
        "tags": {"project": "Alpha", "env": "dev"},
        "libraries": [{"jar": "dbfs:/Shared/jars/custom.jar"}, {"pypi": {"package": "pandas==1.5.0"}}]
    }
    sample_history_analysis = {
        "total_runs_analyzed": 5,
        "period_start_utc": "2023-05-01 00:00:00 UTC", "period_end_utc": "2023-05-05 00:00:00 UTC",
        "duration_stats_str": {"min": "1m 0.00s", "max": "5m 0.00s", "mean": "3m 0.00s", "median": "3m 0.00s", "std_dev": "1m 24.90s"},
        "success_rate_percent": "60.00%",
        "status_counts": {"SUCCESS": 3, "FAILED": 1, "CANCELED": 1},
        "notable_runs": {
            "longest": [{"run_id": 104, "duration_str": "5m 0.00s", "start_time_utc": "2023-05-04 19:00", "result_state": "SUCCESS"}],
            "shortest": [{"run_id": 103, "duration_str": "1m 0.00s", "start_time_utc": "2023-05-03 00:00", "result_state": "FAILED"}],
            "recent_failures": [{"run_id": 103, "duration_str": "1m 0.00s", "start_time_utc": "2023-05-03 00:00", "result_state": "FAILED"}]
        }
    }
    
    dummy_bar_chart = "dummy_bar.png"; dummy_pie_chart = "dummy_pie.png"
    try:
        from PIL import Image as PILImage, ImageDraw
        def create_dummy_png(filepath, text):
            img = PILImage.new('RGB', (650, 390), color = (230, 230, 230)); d = ImageDraw.Draw(img)
            try: d.text((20,20), text, fill=(0,0,0))
            except TypeError: d.text((20,20), text) # Older Pillow
            img.save(filepath)
        create_dummy_png(dummy_bar_chart, "Bar Chart: Run Durations"); create_dummy_png(dummy_pie_chart, "Pie Chart: Run Statuses")
        print(f"Created dummy images: {dummy_bar_chart}, {dummy_pie_chart}")

        report_path = pdf_gen.generate_report(
            job_id="TEST_JOB_123",
            processed_job_details=sample_job_details,
            run_history_analysis=sample_history_analysis,
            bar_chart_path=dummy_bar_chart,
            pie_chart_path=dummy_pie_chart
        )
        if report_path: print(f"Test PDF report generated: {report_path}")
        else: print("Failed to generate test PDF report.")
        if os.path.exists(dummy_bar_chart): os.remove(dummy_bar_chart)
        if os.path.exists(dummy_pie_chart): os.remove(dummy_pie_chart)
    except ImportError:
        print("Pillow library not found or error in dummy image creation. PDF test will proceed without images if paths are None.")
        report_path = pdf_gen.generate_report(job_id="TEST_JOB_NO_IMG", processed_job_details=sample_job_details, run_history_analysis=sample_history_analysis)
        if report_path: print(f"Test PDF report (no images) generated: {report_path}")