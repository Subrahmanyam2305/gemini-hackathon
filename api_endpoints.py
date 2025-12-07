"""
API endpoints for DAG monitoring and fix proposal management
"""
from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import json
import os
import logging
import re
from dotenv import load_dotenv
from google import genai
from google.genai import types

from .db_models import dag_db
from .sqs_service import sqs_service

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger("marathon-agent-api")
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/dag", tags=["dag_monitoring"])

# Initialize Gemini API
try:
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        os.environ['GEMINI_API_KEY'] = api_key
        logger.info("Gemini API configured successfully")
    else:
        logger.warning("No GEMINI_API_KEY found in environment")
except Exception as e:
    logger.error(f"Error configuring Gemini API: {str(e)}")

# Models
class ErrorRegistration(BaseModel):
    dag_id: str
    error_type: str
    error_message: str
    log_data: Optional[Dict[str, Any]] = None
    s3_file_path: Optional[str] = None

class ProposalApproval(BaseModel):
    reviewer_comments: Optional[str] = ""

# Routes
@router.get("/analyze")
async def analyze_dags():
    """Analyze DAG files for issues and register them in the database"""
    try:
        from .local_dag_analyzer import dag_analyzer
        error_ids = dag_analyzer.register_detected_issues()
        
        # Send each error to SQS
        for error_id in error_ids:
            errors = dag_db.get_dag_errors(limit=1000)
            error = next((e for e in errors if e['id'] == error_id), None)
            
            if error:
                # Send message to SQS
                sqs_service.send_error_message({
                    'error_id': error_id,
                    'dag_id': error['dag_id'],
                    'error_type': error['error_type'],
                    'error_message': error['error_message'],
                    'status': 'detected',
                    'state': error['state']
                })
        
        return {"message": f"Analyzed DAGs and found {len(error_ids)} issues", "error_ids": error_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing DAGs: {str(e)}")

@router.get("/errors")
async def get_dag_errors(
    state: Optional[str] = None,
    dag_id: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """Get DAG errors"""
    try:
        errors = dag_db.get_dag_errors(state=state, dag_id=dag_id, limit=limit)
        return errors
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving DAG errors: {str(e)}")

@router.post("/errors")
async def register_dag_error(error: ErrorRegistration):
    """Register a new DAG error"""
    try:
        log_data = json.dumps(error.log_data) if error.log_data else None
        error_id = dag_db.add_dag_error(
            dag_id=error.dag_id,
            error_type=error.error_type,
            error_message=error.error_message,
            log_data=log_data,
            s3_file_path=error.s3_file_path
        )
        
        if error_id < 0:
            raise HTTPException(status_code=500, detail="Failed to register error")
            
        # Send message to SQS
        sqs_service.send_error_message({
            'error_id': error_id,
            'dag_id': error.dag_id,
            'error_type': error.error_type,
            'error_message': error.error_message,
            'status': 'registered',
            'state': 'needs_processing'
        })
            
        return {"id": error_id, "message": "Error registered successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error registering DAG error: {str(e)}")

@router.get("/errors/{error_id}/analyze")
async def analyze_error(error_id: int):
    """Analyze an error and generate fix proposal using Gemini"""
    try:
        # Get the error details
        errors = dag_db.get_dag_errors(limit=1000)
        error = next((e for e in errors if e['id'] == error_id), None)
        
        if not error:
            raise HTTPException(status_code=404, detail=f"Error {error_id} not found")
        
        # Get log details
        log_details = {}
        if 'log_data' in error and error['log_data']:
            try:
                log_details = json.loads(error['log_data'])
            except:
                pass
        
        # Extract error information
        dag_id = error['dag_id']
        error_type = error['error_type']
        error_message = error['error_message']
        code_context = log_details.get('code_context', '')
        
        # Generate a fix proposal using Gemini
        analysis, fix_suggestion, fixed_code = await generate_fix_with_gemini(
            dag_id, error_type, error_message, code_context
        )
        
        # If Gemini couldn't provide fixes, use the ones from log_details
        if not fix_suggestion and 'suggested_fix' in log_details:
            fix_suggestion = log_details.get('suggested_fix', '')
            
        if not fixed_code and 'fixed_code' in log_details:
            fixed_code = log_details.get('fixed_code', '')
        
        # Add the fix proposal to the database
        proposal_id = dag_db.add_fix_proposal(
            error_id=error_id,
            analysis=analysis,
            fix_suggestion=fix_suggestion,
            code_changes=fixed_code
        )
        
        if proposal_id < 0:
            raise HTTPException(status_code=500, detail="Failed to create fix proposal")
        
        # Send message to SQS about the new proposal
        sqs_service.send_error_message({
            'error_id': error_id,
            'proposal_id': proposal_id,
            'dag_id': dag_id,
            'error_type': error_type,
            'status': 'analyzed',
            'state': 'awaiting_approval'
        })
            
        return {
            "id": proposal_id,
            "error_id": error_id,
            "message": "Fix proposal generated successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing DAG error: {str(e)}")

async def generate_fix_with_gemini(dag_id: str, error_type: str, error_message: str, code_context: str):
    """Generate a fix using Gemini API"""
    try:
        if not os.getenv("GEMINI_API_KEY"):
            logger.warning("No Gemini API key available, using fallback analysis")
            return (
                f"Analysis of the {error_type} error in {dag_id}",
                f"Suggested fix for {error_type}",
                code_context
            )
        
        # Create the prompt
        prompt = f"""
        You are a Python code repair expert specializing in Airflow DAGs and data pipelines.
        
        I need your help to fix an error in a DAG file. Here's the information:
        
        DAG ID: {dag_id}
        Error Type: {error_type}
        Error Message: {error_message}
        
        Here's the problematic code:
        
        ```python
        {code_context}
        ```
        
        Please analyze this code and provide three specific outputs:
        1. A brief analysis of what's causing the issue (2-3 sentences)
        2. A suggested fix explanation in plain language 
        3. The complete fixed code that resolves the issue
        
        Your response should contain these three sections clearly labeled as:
        ANALYSIS: (your analysis here)
        FIX SUGGESTION: (your suggested fix here)
        FIXED CODE: (the corrected code here)
        """
        
        # Call Gemini API using Client() as requested
        client = genai.Client()
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[prompt],
            config=types.GenerateContentConfig(
                temperature=0.2,
                top_p=0.95,
                top_k=40,
                max_output_tokens=4096
            )
        )
        
        # Get the raw text response
        response_text = response.text
        logger.info(f"Gemini response received: {response_text[:100]}...")
        
        # Extract sections using regex patterns instead of JSON parsing
        analysis_match = re.search(r"(?:ANALYSIS:|Analysis:)\s*(.*?)(?:FIX SUGGESTION:|Fix Suggestion:|$)", 
                                  response_text, re.DOTALL | re.IGNORECASE)
        
        fix_suggestion_match = re.search(r"(?:FIX SUGGESTION:|Fix Suggestion:)\s*(.*?)(?:FIXED CODE:|Fixed Code:|$)", 
                                        response_text, re.DOTALL | re.IGNORECASE)
        
        fixed_code_match = re.search(r"(?:FIXED CODE:|Fixed Code:)\s*(.*?)$", 
                                    response_text, re.DOTALL | re.IGNORECASE)
        
        # Extract the matches or use defaults
        analysis = analysis_match.group(1).strip() if analysis_match else f"Analysis of the {error_type} error in {dag_id}"
        fix_suggestion = fix_suggestion_match.group(1).strip() if fix_suggestion_match else f"Suggested fix for {error_type}"
        fixed_code = fixed_code_match.group(1).strip() if fixed_code_match else code_context
        
        # Try to clean up the code if it has markdown code blocks
        if fixed_code.startswith("```python"):
            fixed_code = re.sub(r"```python\n(.*?)\n```", r"\1", fixed_code, flags=re.DOTALL)
        elif fixed_code.startswith("```"):
            fixed_code = re.sub(r"```\n(.*?)\n```", r"\1", fixed_code, flags=re.DOTALL)
        
        # Log what we extracted
        logger.info(f"Extracted analysis: {analysis[:50]}...")
        logger.info(f"Extracted fix suggestion: {fix_suggestion[:50]}...")
        logger.info(f"Extracted fixed code length: {len(fixed_code)} characters")
        
        return analysis, fix_suggestion, fixed_code
        
    except Exception as e:
        logger.error(f"Error calling Gemini API: {str(e)}")
        return (
            f"Analysis of the {error_type} error in {dag_id}",
            f"Suggested fix for {error_type}",
            code_context
        )

@router.get("/proposals")
async def get_fix_proposals(
    state: Optional[str] = None,
    error_id: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """Get fix proposals"""
    try:
        proposals = dag_db.get_fix_proposals(state=state, error_id=error_id, limit=limit)
        return proposals
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving fix proposals: {str(e)}")

@router.post("/proposals/{proposal_id}/approve")
async def approve_fix_proposal(proposal_id: int, approval: ProposalApproval = Body(...)):
    """Approve a fix proposal"""
    try:
        success = dag_db.approve_fix(
            proposal_id=proposal_id,
            reviewer="Web UI",
            comments=approval.reviewer_comments
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to approve fix proposal")
        
        # Get proposal and error details
        proposals = dag_db.get_fix_proposals()
        proposal = next((p for p in proposals if p['id'] == proposal_id), None)
        
        if proposal:
            # Send approval message to SQS
            sqs_service.send_approval_message({
                'proposal_id': proposal_id,
                'error_id': proposal['error_id'],
                'dag_id': proposal['dag_id'],
                'status': 'approved',
                'reviewer_comments': approval.reviewer_comments,
                'action': 'approved'
            })
            
        return {"message": "Fix proposal approved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error approving fix proposal: {str(e)}")

@router.post("/proposals/{proposal_id}/reject")
async def reject_fix_proposal(proposal_id: int, approval: ProposalApproval = Body(...)):
    """Reject a fix proposal"""
    try:
        success = dag_db.reject_fix(
            proposal_id=proposal_id,
            reviewer="Web UI",
            comments=approval.reviewer_comments
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to reject fix proposal")
        
        # Get proposal and error details
        proposals = dag_db.get_fix_proposals()
        proposal = next((p for p in proposals if p['id'] == proposal_id), None)
        
        if proposal:
            # Send rejection message to SQS
            sqs_service.send_approval_message({
                'proposal_id': proposal_id,
                'error_id': proposal['error_id'],
                'dag_id': proposal['dag_id'],
                'status': 'rejected',
                'reviewer_comments': approval.reviewer_comments,
                'action': 'rejected'
            })
            
        return {"message": "Fix proposal rejected successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error rejecting fix proposal: {str(e)}")

@router.get("/sqs/status")
async def sqs_status():
    """Get SQS status"""
    return {
        "error_queue": sqs_service.error_queue_name,
        "error_queue_url": sqs_service.error_queue_url,
        "approval_queue": sqs_service.approval_queue_name,
        "approval_queue_url": sqs_service.approval_queue_url,
        "status": "connected" if sqs_service.sqs_client else "disconnected"
    }

# Add a simple placeholder for Gemini agent
class GeminiAgentPlaceholder:
    def analyze_error(self, error):
        return "Placeholder Gemini error analysis"
    
    def generate_fix(self, error):
        return "Placeholder Gemini fix"

# Use a placeholder gemini_agent if the real one isn't available
try:
    from .gemini_agent import gemini_agent
except ImportError:
    gemini_agent = GeminiAgentPlaceholder()