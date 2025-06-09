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
    code_language: str = Field(..., description="Language of the code (e.g., 'python_spark', 'sql', 'scala_spark').")
    job_context: Optional[JobRunContext] = None


# --- Response Models (from FastAPI to CLI, based on Gemini's output) ---

class AlternativeApproachSuggestion(BaseModel):
    title: str = "Consider an Alternative High-Level Approach"
    description: str = Field(..., description="Explanation of why an alternative approach might be significantly better.")
    suggested_approach_overview: List[str] = Field(..., description="High-level steps or structural changes for the new approach.")
    estimated_benefits: List[str] = Field(..., description="Potential significant improvements (e.g., 'Reduced data shuffle by X%', 'Lowered compute cost by Y%').")

class CodeBlockOptimizationSuggestion(BaseModel):
    block_id: str = Field(..., description="Identifier for the code block/cell (e.g., cell number, function name, or a snippet hash).")
    problematic_code_snippet: Optional[str] = Field(None, description="The specific snippet of code (or a significant part of it) that has an inefficiency.")
    inefficiency_summary: str = Field(..., description="A concise summary of the identified inefficiency.")
    detailed_explanation: str = Field(..., description="A more detailed explanation of why it's inefficient.")
    improvement_suggestion_conceptual: str = Field(..., description="Conceptual suggestion on how to improve it (what to do, design change).")
    potential_impact_level: Optional[str] = Field(None, description="Estimated potential positive impact: 'High', 'Medium', or 'Low'") # ADDED

class AnalysisReport(BaseModel):
    request_id: str
    overall_assessment: str = Field(..., description="A brief summary of the code's purpose and general findings.")
    alternative_approach: Optional[AlternativeApproachSuggestion] = None
    code_block_suggestions: List[CodeBlockOptimizationSuggestion] = Field(default_factory=list)
    common_inefficiencies_observed: List[str] = Field(default_factory=list, description="A list of common types of inefficiencies observed across the codebase (e.g., 'Frequent use of collect()', 'Inefficient UDFs').")