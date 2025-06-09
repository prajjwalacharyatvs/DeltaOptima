from dotenv import load_dotenv
import os

load_dotenv()
from fastapi import FastAPI, HTTPException, status
from .models import CodeAnalysisRequest, AnalysisReport
from . import gemini_service

app = FastAPI(
    title="pipeLiner Analysis Service",
    description="Receives Databricks code and context, and returns optimization suggestions from Gemini.",
    version="0.1.0",
)

@app.get("/", tags=["General"], include_in_schema=False)
async def read_root():
    return {"message": "Welcome to the pipeLiner Analysis Service!", "status": "healthy"}

@app.post(
    "/api/v1/analyze/",
    response_model=AnalysisReport,
    tags=["Analysis"],
    status_code=status.HTTP_200_OK
)
async def analyze_databricks_code(request: CodeAnalysisRequest) -> AnalysisReport:
    """
    Receives Databricks notebook/job context and code for analysis.
    Processes it using Gemini and returns structured optimization suggestions.
    """
    print(f"FastAPI: Received analysis request ID: {request.request_id}")
    if not request.code_content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Code content cannot be empty."
        )

    try:
        analysis_report = await gemini_service.get_optimization_suggestions(request)
        print(f"FastAPI: Returning analysis report for request ID: {request.request_id}")
        return analysis_report
    except Exception as e:
        print(f"[ERROR] FastAPI - Unhandled error in analysis endpoint for request {request.request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected internal error occurred: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    print("Running FastAPI app with Uvicorn from __main__ (for simple testing only)...")
    uvicorn.run(app, host="0.0.0.0", port=8000)