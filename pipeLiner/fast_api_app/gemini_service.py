import os
import json
from typing import Dict, Any, List, Optional
import google.generativeai as genai
import asyncio 

from .models import CodeAnalysisRequest, AnalysisReport, AlternativeApproachSuggestion, CodeBlockOptimizationSuggestion

# --- Gemini API Configuration ---
GEMINI_API_KEY = os.getenv("PIPE_LINER_GEMINI_API_KEY")

if not GEMINI_API_KEY:
    print("[CRITICAL][gemini_service] PIPE_LINER_GEMINI_API_KEY not found in environment variables! The service will not be able to contact Gemini.")
else:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        print("[INFO][gemini_service] Gemini API key configured.")
    except Exception as e:
        print(f"[CRITICAL][gemini_service] Failed to configure Gemini API key: {e}")
        GEMINI_API_KEY = None 

MODEL_NAME = "gemini-2.0-flash"

DEBUG_GEMINI_CALL = os.getenv("PIPE_LINER_DEBUG_GEMINI", "false").lower() == "true"


def _build_prompt(request_data: CodeAnalysisRequest) -> str:
    """Helper function to construct the detailed prompt for Gemini."""
    job_context_str = "No specific job context was provided with this code."
    if request_data.job_context:
        jc = request_data.job_context
        cluster_str = "N/A"
        if jc.cluster_info:
            ci = jc.cluster_info
            cluster_str = (
                f"Spark Version: {ci.spark_version or 'N/A'}, "
                f"Node Type (Workers): {ci.node_type_id or 'N/A'}, "
                f"Driver Node Type: {ci.driver_node_type_id or ci.node_type_id or 'N/A'}, "
                f"Number of Workers: {ci.num_workers if ci.num_workers is not None else 'N/A'}, "
                f"Runtime Engine: {ci.runtime_engine or 'STANDARD'}, "
                f"Cloud Platform: {ci.cloud_platform or 'N/A'}."
            )

        tasks_summary_list = []
        if jc.tasks:
            for task in jc.tasks:
                tasks_summary_list.append(
                    f"  - Task Key: '{task.task_key}', Type: {task.task_type or 'N/A'}, "
                    f"Duration: {f'{task.execution_duration_seconds:.2f}s' if task.execution_duration_seconds is not None else 'N/A'}, "
                    f"Result: {task.result_state or 'N/A'}."
                )
        tasks_summary_str = "\n".join(tasks_summary_list) if tasks_summary_list else "  No specific task details provided."

        job_context_str = (
            f"Job ID: {jc.job_id or 'N/A'}\n"
            f"Run ID: {jc.run_id or 'N/A'}\n"
            f"Run Name: {jc.run_name or 'N/A'}\n"
            f"Overall Run Duration: {f'{jc.overall_run_duration_seconds:.2f}s' if jc.overall_run_duration_seconds is not None else 'N/A'}\n"
            f"Trigger Type: {jc.trigger_type or 'N/A'}\n"
            f"Cluster Configuration: {cluster_str}\n"
            f"Task Summary:\n{tasks_summary_str}"
        )

    json_output_format_description = """
Your entire response MUST be a single, valid JSON object. Do NOT include any text, explanations, or markdown formatting (like ```json) outside of this JSON object.
The JSON object should have the following top-level keys: "overall_assessment", "alternative_approach" (optional), "code_block_suggestions", and "common_inefficiencies_observed".

Structure:
{
  "overall_assessment": "string (A brief summary of the code's purpose, its main ETL steps, and general findings regarding its current efficiency and clarity. Max 3-4 sentences.)",
  "alternative_approach": { // Optional: Include this object ONLY IF a fundamentally different high-level approach could yield *significant* (e.g., >30-50% improvement in key metrics) and practical benefits. Be conservative; if unsure, omit it.
    "title": "string (e.g., 'Consider a Streaming ETL Approach with Delta Live Tables')",
    "description": "string (Concise explanation of why this alternative approach is compelling for this specific use case and its major advantages over the current batch approach.)",
    "suggested_approach_overview": ["string", "string", ... (Key high-level steps or architectural changes for the new approach. Focus on the 'what', not deep 'how'.)],
    "estimated_benefits": ["string", "string", ... (Quantifiable or qualitative significant improvements, e.g., 'Reduced data latency from hours to minutes', 'Simplified data pipeline management', 'Potentially lower compute for incremental updates.')]
  },
  "code_block_suggestions": [ // An array of objects. Create one object for each distinct code block/cell where you identify one or more clear, actionable inefficiencies.
    {
      "block_id": "string (Identify the block, e.g., 'Cell X based on # COMMAND ---------- X', or a descriptive name like 'Data Ingestion for fact_cal'. If multiple issues in one cell, list them under one block_id or create separate suggestions if conceptually distinct.)",
      "problematic_code_snippet": "string (A short, relevant snippet from the original code (max 5-10 lines) that clearly shows the inefficiency. If the whole block is problematic, summarize its action.)",
      "inefficiency_summary": "string (A concise (1-sentence) summary of the core inefficiency in this block.)",
      "detailed_explanation": "string (A thorough explanation (3-5 sentences or more if needed) of *why* this is inefficient in the context of Spark/Databricks. Discuss impact on Spark's execution model, memory, shuffle, I/O, or potential for errors/data quality issues.)",
      "improvement_suggestion_conceptual": "string (Provide a detailed conceptual suggestion on *what* to do differently or *what concept* to apply. Explain the rationale for the suggestion. If multiple conceptual improvements exist for the identified inefficiency, you can list them or briefly describe them. Do NOT provide full replacement code blocks.)",
      "potential_impact_level": "string (Estimate the potential positive impact of addressing this inefficiency: 'High', 'Medium', or 'Low')"
    }
    // Provide a comprehensive analysis for each block where significant issues are found.
    // If a similar inefficiency (e.g., repartition(1)) appears in multiple cells, provide one or two detailed representative examples in `code_block_suggestions`
    // and then robustly list the general pattern in `common_inefficiencies_observed` mentioning its pervasiveness.
  ],
  "common_inefficiencies_observed": ["string", "string", ... (A bulleted-style list of common anti-patterns or types of inefficiencies observed across multiple parts of the codebase. Be specific, e.g., 'Widespread use of .repartition(1) before writes, impacting X, Y, Z cells', 'Inconsistent date/string formatting leading to repeated transformations in cells A, B, C'.)]
}
"""

    prompt = f"""As an expert Databricks and Apache Spark optimization assistant, your primary goal is to perform a **deep and thorough analysis** of the provided {request_data.code_language} code and its execution context. Identify inefficiencies and suggest specific, actionable improvements to enhance efficiency (speed, resource usage, cost) and maintainability.

**Code to Analyze:**
{request_data.code_content}


**Execution Context (if available, use this to tailor suggestions):**
{job_context_str}

**Analysis Instructions & Output Requirements:**

1.  **Holistic Understanding:** First, thoroughly understand the overall purpose, data flow, and functionality of the provided code. Summarize this understanding concisely in your `overall_assessment`.
2.  **Alternative Approach (Conditional and High-Impact Only):** If, and only if, you identify a fundamentally different high-level architectural approach (e.g., switching from batch to streaming, using Delta Live Tables, significantly different data modeling for the core problem) that would offer **substantial and practical** efficiency gains (time, cost, resources), describe this in the `alternative_approach` section of the JSON. Be specific about the suggested approach and its key benefits. If no such high-impact alternative is evident or justifiable, **omit the `alternative_approach` key entirely or set its value to null in the JSON.**
3.  **Deep Code Block Specific Analysis & Suggestions:**
    * Analyze the provided code cell by cell (for notebooks, assume `# COMMAND ----------` separates cells effectively) or by logical block.
    * For **each block** where you identify **one or more clear, actionable inefficiencies** (related to Spark/Databricks best practices, performance, cost, or significant maintainability issues that affect performance), create a detailed object in the `code_block_suggestions` array.
    * For each suggestion object, provide the fields as described in the JSON structure below (`block_id`, `problematic_code_snippet`, `inefficiency_summary`, `detailed_explanation`, `improvement_suggestion_conceptual`, `potential_impact_level`).
    * **Thoroughness:** Aim for a comprehensive analysis of problematic blocks. If a block has multiple distinct inefficiencies, you can create multiple suggestion objects for that same `block_id` or detail them within a single suggestion's explanation if they are closely related.
    * **Conceptual Suggestions:** For `improvement_suggestion_conceptual`, explain *what* to do or *what concept* to apply. **Do NOT provide complete rewritten code blocks.**
4.  **Common Inefficiencies Summary:** In the `common_inefficiencies_observed` array, list general types of anti-patterns or recurring inefficiencies observed throughout the codebase. If an issue like `repartition(1)` is pervasive, mention it here with an indication of its frequency or scope, even if you've detailed specific instances in `code_block_suggestions`.

**VERY IMPORTANT: Your entire response MUST be a single, valid JSON object matching the precise structure described below. Do not include any introductory text, concluding remarks, or markdown formatting (like ```json) outside of this JSON object.**

{json_output_format_description}

Perform a deep analysis now and provide your structured JSON response.
"""
    return prompt


async def get_optimization_suggestions(request_data: CodeAnalysisRequest) -> AnalysisReport:
    if not GEMINI_API_KEY:
        print("[WARN][gemini_service] Gemini API Key not configured. Returning a placeholder error report.")
        # Return a valid AnalysisReport indicating the error
        return AnalysisReport(
            request_id=request_data.request_id or "no-request-id-provided",
            overall_assessment="Error: Gemini API key is not configured in the FastAPI server environment. Please set PIPE_LINER_GEMINI_API_KEY.",
            code_block_suggestions=[],
            common_inefficiencies_observed=["API Key Missing - Unable to perform analysis."]
        )

    full_prompt = _build_prompt(request_data)

    if DEBUG_GEMINI_CALL:
        print("\n" + "="*40 + " PROMPT SENT TO GEMINI " + "="*40)
        print(full_prompt)
        print("="*40 + " END OF PROMPT " + "="*40 + "\n")

    raw_response_text: str = "" # Initialize to ensure it's always defined
    try:
        model = genai.GenerativeModel(MODEL_NAME)
        
        generation_config_params = {}
        
        generation_config = genai.types.GenerationConfig(**generation_config_params) if generation_config_params else None


        print(f"[INFO][gemini_service] Sending request to Gemini model: {MODEL_NAME}...")
        response = await model.generate_content_async(
            full_prompt,
            generation_config=generation_config
        )
        
        raw_response_text = response.text
        if DEBUG_GEMINI_CALL:
            print("\n" + "="*40 + " RAW RESPONSE FROM GEMINI " + "="*40)
            print(raw_response_text)
            print("="*40 + " END OF RAW RESPONSE " + "="*40 + "\n")

        cleaned_json_text = raw_response_text.strip()
        if cleaned_json_text.startswith("```json"):
            cleaned_json_text = cleaned_json_text[len("```json"):]
        if cleaned_json_text.endswith("```"):
            cleaned_json_text = cleaned_json_text[:-len("```")]
        cleaned_json_text = cleaned_json_text.strip()
        
        if not cleaned_json_text:
            raise ValueError("Gemini returned an empty response after cleaning.")

        gemini_output_dict = json.loads(cleaned_json_text)

        analysis_report = AnalysisReport(
            request_id=request_data.request_id or "unknown-request-id",
            overall_assessment=gemini_output_dict.get("overall_assessment", "No overall assessment provided by Gemini."),
            alternative_approach=(
                AlternativeApproachSuggestion(**gemini_output_dict["alternative_approach"])
                if gemini_output_dict.get("alternative_approach") else None
            ),
            code_block_suggestions=[
                CodeBlockOptimizationSuggestion(**sug)
                for sug in gemini_output_dict.get("code_block_suggestions", [])
            ],
            common_inefficiencies_observed=gemini_output_dict.get("common_inefficiencies_observed", [])
        )
        print(f"[INFO][gemini_service] Successfully processed Gemini response for request ID: {analysis_report.request_id}")
        return analysis_report

    except json.JSONDecodeError as e:
        error_message = f"Failed to decode JSON response from Gemini: {e}. Ensure Gemini returns valid JSON as per the prompt instructions."
        print(f"[ERROR][gemini_service] {error_message}")
        print(f"Gemini raw response was: {raw_response_text}")
        return AnalysisReport(
            request_id=request_data.request_id or "error-decoding-json",
            overall_assessment=error_message,
            common_inefficiencies_observed=[f"Raw Gemini output snippet (check server logs for full output): {raw_response_text[:500]}"]
        )
    except Exception as e:
        error_message = f"An unexpected error occurred while interacting with Gemini or processing its response: {type(e).__name__} - {e}"
        print(f"[ERROR][gemini_service] {error_message}")
        import traceback
        traceback.print_exc() 
        return AnalysisReport(
            request_id=request_data.request_id or "error-unexpected",
            overall_assessment=error_message,
            common_inefficiencies_observed=[]
        )
