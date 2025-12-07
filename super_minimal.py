"""
Super minimal FastAPI application with DAG monitoring integration
"""
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import os
import json
import asyncio
import logging
import traceback
from typing import Dict, Any, Optional, AsyncIterator
from dotenv import load_dotenv
from google import genai
from google.genai import types

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("marathon-agent-api")

# Simple configuration using environment variables
class SimpleConfig:
    def __init__(self):
        self.GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
        self.GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")
        self.DB_TYPE = os.getenv("DB_TYPE", "sqlite")
        self.SQLITE_DB = os.getenv("SQLITE_DB", "marathon_agent.db")
        self.ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
        
        # Log if Gemini API key was found
        if self.GEMINI_API_KEY:
            logger.info("Gemini API Key found in environment")
        else:
            logger.warning("No Gemini API Key found in environment")

# Create config instance
config = SimpleConfig()

# Create FastAPI app
app = FastAPI(
    title="Marathon Agent API",
    description="API for the Marathon Agent with Gemini integration and DAG monitoring",
    version="0.3.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import and include the DAG monitoring API router
from .api_endpoints import router as dag_router
app.include_router(dag_router)

@app.get("/health")
def health_check():
    return {
        "status": "healthy", 
        "db_type": config.DB_TYPE,
        "gemini_model": config.GEMINI_MODEL_NAME,
        "dag_monitoring": True
    }

@app.options("/health")
async def health_options():
    """Handle OPTIONS requests to /health endpoint for CORS preflight"""
    return {}

@app.post("/ask-gemini")
async def ask_gemini(question: Dict[str, Any]):
    """
    Simple endpoint to test Gemini API integration
    """
    api_key = config.GEMINI_API_KEY
    if not api_key:
        logger.error("GEMINI_API_KEY not set in environment variables")
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY not set in environment variables")
    
    prompt = question.get("prompt")
    if not prompt:
        logger.error("No prompt provided in request")
        raise HTTPException(status_code=400, detail="No prompt provided in request")
    
    # Check if we should use DAG monitoring context
    use_dag_context = question.get("use_dag_context", False)
    dag_id = question.get("dag_id")
    
    # Add DAG context if requested
    if use_dag_context and dag_id:
        from .db_models import dag_db
        
        # Get recent errors for this DAG
        errors = [e for e in dag_db.get_errors_for_processing(limit=100) if e['dag_id'] == dag_id]
        
        if errors:
            # Add context from the most recent error
            latest_error = errors[0]
            prompt = f"""Context: The following error was detected in DAG {dag_id}:
Error Type: {latest_error['error_type']}
Error Message: {latest_error['error_message']}
Log Data: {latest_error.get('log_data', '')}

User Query: {prompt}"""
    
    stream_mode = question.get("stream", True)
    
    logger.info(f"Processing prompt: {prompt[:50]}... (Stream mode: {stream_mode})")
    
    # Format with RPG style for the Marathon Agent theme
    formatted_prompt = f"""You are Marathon Agent, an AI assistant with an RPG fantasy-themed personality.
    
    You should respond to the user's question with helpful information while maintaining your role as a 
    magical advisor monitoring a data realm. Use fantasy metaphors like "realms", "quests", and "spells" 
    to describe technical concepts.
    
    User query: {prompt}
    
    Respond in character as Marathon Agent:"""
    
    if stream_mode:
        return StreamingResponse(
            generate_streaming_response(formatted_prompt),
            media_type="text/event-stream"
        )
    else:
        try:
            # Initialize the client
            os.environ['GEMINI_API_KEY'] = api_key
            client = genai.Client()
            
            # Generate content using the correct pattern
            response = client.models.generate_content(
                model=config.GEMINI_MODEL_NAME,
                contents=[formatted_prompt],
                config=types.GenerateContentConfig(
                    temperature=0.7,
                    top_p=0.95,
                    top_k=40,
                    max_output_tokens=2048
                )
            )
            
            # Check if we have a valid response
            if not hasattr(response, 'text') or not response.text:
                logger.error("Empty response from Gemini API")
                return {
                    "response": "I apologize, but I couldn't formulate a proper response at this moment. Please try again.",
                    "error": "Empty response from Gemini API"
                }
            
            logger.info(f"Generated response: {response.text[:50]}...")
            
            # Store interaction in SQLite if available
            try:
                store_in_sqlite(formatted_prompt, response.text)
            except Exception as db_error:
                logger.error(f"Error storing in SQLite: {str(db_error)}")
            
            return {"response": response.text}
        
        except Exception as e:
            error_details = traceback.format_exc()
            logger.error(f"Error calling Gemini API: {str(e)}")
            logger.error(error_details)
            return {
                "response": "I apologize, but I encountered an error while processing your request. Please try again or check the API configuration.",
                "error": str(e)
            }

async def generate_streaming_response(prompt: str) -> AsyncIterator[str]:
    """Generate a streaming response from Gemini API"""
    try:
        # Initialize the client
        os.environ['GEMINI_API_KEY'] = config.GEMINI_API_KEY
        client = genai.Client()
        
        # Use stream generation
        response_stream = client.models.generate_content_stream(
            model=config.GEMINI_MODEL_NAME,
            contents=[prompt],
        )
        
        # Send SSE formatted messages
        full_response = ""
        
        # Send the opening of the JSON
        yield 'data: {"type":"start"}\n\n'
        
        for chunk in response_stream:
            if hasattr(chunk, 'text') and chunk.text:
                text_chunk = chunk.text
                full_response += text_chunk
                # Format as SSE message
                yield f'data: {{"type":"chunk", "content":{json.dumps(text_chunk)}}}\n\n'
                # Small delay to simulate streaming
                await asyncio.sleep(0.01)
        
        # Store the full response in SQLite
        try:
            store_in_sqlite(prompt, full_response)
        except Exception as db_error:
            logger.error(f"Error storing streaming response in SQLite: {str(db_error)}")
        
        # Send the closing JSON
        yield 'data: {"type":"end"}\n\n'
        
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error in streaming Gemini API response: {str(e)}")
        logger.error(error_details)
        yield f'data: {{"type":"error", "content":{json.dumps(str(e))}}}\n\n'
        yield 'data: {"type":"end"}\n\n'

def store_in_sqlite(prompt, response):
    """Store prompt and response in SQLite"""
    if config.DB_TYPE != "sqlite":
        return
    
    import sqlite3
    import time
    
    try:
        conn = sqlite3.connect(config.SQLITE_DB)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS gemini_interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt TEXT,
            response TEXT,
            timestamp TEXT
        )
        ''')
        
        # Insert the prompt and response
        cursor.execute(
            "INSERT INTO gemini_interactions (prompt, response, timestamp) VALUES (?, ?, ?)",
            (prompt, response, time.strftime("%Y-%m-%d %H:%M:%S"))
        )
        
        conn.commit()
        conn.close()
        
        logger.info("Stored interaction in SQLite database")
    except Exception as e:
        logger.error(f"Error storing in SQLite: {str(e)}")
        raise

@app.options("/ask-gemini")
async def ask_gemini_options():
    """Handle OPTIONS requests to /ask-gemini endpoint for CORS preflight"""
    return {}

@app.get("/echo")
async def echo(text: str = Query(..., description="Text to echo back")):
    """
    Simple echo endpoint for testing
    """
    return {"echo": text}

@app.options("/echo")
async def echo_options():
    """Handle OPTIONS requests to /echo endpoint for CORS preflight"""
    return {}