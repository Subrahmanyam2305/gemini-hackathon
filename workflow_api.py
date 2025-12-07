"""
Workflow API endpoints for Marathon Agent
"""
import uuid
import time
import json
from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, Query, Body, Path
from pydantic import BaseModel, Field

# Import database and SQS components
from .db_schema import get_db_manager
from .sqs_publisher import get_sqs_publisher

# Create router
router = APIRouter(prefix="/workflow", tags=["workflow"])

# Schema models for API
class StateTransition(BaseModel):
    state: str = Field(..., description="State name")
    entered_at: str = Field(..., description="When the state was entered")
    exited_at: Optional[str] = Field(None, description="When the state was exited")
    duration_seconds: Optional[int] = Field(None, description="Duration in seconds")
    result: Optional[str] = Field(None, description="Result of the state")
    has_error: bool = Field(False, description="Whether there was an error")
    error_message: Optional[str] = Field(None, description="Error message if any")

class TaskSummary(BaseModel):
    task_id: str
    workflow_id: str
    workflow_name: str
    current_state: str
    created_at: str
    updated_at: str
    is_complete: bool
    has_error: bool

class TaskDetail(TaskSummary):
    workflow_version: str
    completion_state: str
    creator_id: str
    priority: str
    prompt: str
    context: Optional[str]
    parameters: Dict[str, Any]
    results: Dict[str, Any]
    next_states: List[str]
    state_history: List[StateTransition]

class CreateWorkflowRequest(BaseModel):
    prompt: str = Field(..., description="The prompt or query to process")
    workflow_name: str = Field(..., description="Name of the workflow to use")
    initial_state: str = Field("initial", description="Initial state for the workflow")
    context: Optional[str] = Field(None, description="Additional context")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Additional parameters")

class WorkflowResponse(BaseModel):
    task_id: str
    workflow_id: str
    status: str
    message: str

# Endpoints
@router.post("/tasks", response_model=WorkflowResponse)
async def create_workflow_task(request: CreateWorkflowRequest):
    """
    Create a new workflow task
    """
    # Get the SQS publisher
    sqs_publisher = get_sqs_publisher()
    if not sqs_publisher:
        raise HTTPException(status_code=500, detail="SQS publisher not initialized")
    
    # Create the workflow task
    result = sqs_publisher.create_workflow_task(
        workflow_name=request.workflow_name,
        prompt=request.prompt,
        initial_state=request.initial_state,
        context=request.context,
        parameters=request.parameters
    )
    
    # Check if task was created successfully
    if result.get("sqs_result", {}).get("success") is False:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create workflow task: {result.get('sqs_result', {}).get('error')}"
        )
    
    return WorkflowResponse(
        task_id=result.get("task_id"),
        workflow_id=result.get("workflow_id"),
        status="created",
        message="Workflow task created successfully"
    )

@router.get("/tasks", response_model=List[TaskSummary])
async def list_tasks(
    workflow_id: Optional[str] = Query(None, description="Filter by workflow ID"),
    state: Optional[str] = Query(None, description="Filter by current state"),
    limit: int = Query(100, description="Maximum number of tasks to return")
):
    """
    List workflow tasks with optional filtering
    """
    # Get the database manager
    db_manager = get_db_manager()
    if not db_manager:
        raise HTTPException(status_code=500, detail="Database manager not initialized")
    
    # Get tasks from the database
    tasks = db_manager.list_tasks(workflow_id=workflow_id, state=state, limit=limit)
    
    return tasks

@router.get("/tasks/{task_id}", response_model=TaskDetail)
async def get_task(
    task_id: str = Path(..., description="Task ID to retrieve")
):
    """
    Get details for a specific task
    """
    # Get the database manager
    db_manager = get_db_manager()
    if not db_manager:
        raise HTTPException(status_code=500, detail="Database manager not initialized")
    
    # Get the task from the database
    task = db_manager.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    return task

@router.post("/tasks/{task_id}/states/{state}", response_model=WorkflowResponse)
async def update_task_state(
    task_id: str = Path(..., description="Task ID to update"),
    state: str = Path(..., description="New state to transition to"),
    result: Optional[Dict[str, Any]] = Body(None, description="Result data for the state")
):
    """
    Update a task's state manually
    """
    # Get the database manager and SQS publisher
    db_manager = get_db_manager()
    sqs_publisher = get_sqs_publisher()
    
    if not db_manager:
        raise HTTPException(status_code=500, detail="Database manager not initialized")
        
    if not sqs_publisher:
        raise HTTPException(status_code=500, detail="SQS publisher not initialized")
    
    # Get the task from the database
    task = db_manager.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    # Update the task state
    current_state = task.get("current_state")
    workflow_id = task.get("workflow_id")
    
    # Publish state update to SQS
    result = sqs_publisher.update_task_state(
        task_id=task_id,
        workflow_id=workflow_id,
        current_state=current_state,
        new_state=state,
        result=result
    )
    
    # Check if update was published successfully
    if result.get("sqs_result", {}).get("success") is False:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to update task state: {result.get('sqs_result', {}).get('error')}"
        )
    
    return WorkflowResponse(
        task_id=task_id,
        workflow_id=workflow_id,
        status="updated",
        message=f"Task state updated from {current_state} to {state}"
    )
