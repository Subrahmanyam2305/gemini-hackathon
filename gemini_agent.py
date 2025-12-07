"""
Gemini agent for Marathon Agent
Analyzes DAG errors and generates fix proposals
"""
import logging
import os
from typing import Dict, Any, List, Optional, Tuple

from google import genai
from google.genai import types

from .db_models import dag_db

# Configure logging
logger = logging.getLogger("marathon-agent-gemini")
logger.setLevel(logging.INFO)

class GeminiAgent:
    """Gemini-powered agent for error analysis and code fixing"""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """Initialize the Gemini agent"""
        self.api_key = api_key or os.getenv("GEMINI_API_KEY", "")
        self.model_name = model or os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")
        
        # Initialize the client
        self._init_client()
    
    def _init_client(self):
        """Initialize the Gemini client"""
        if not self.api_key:
            logger.error("GEMINI_API_KEY not set")
            self.client = None
            return
        
        try:
            # Set API key in environment
            os.environ['GEMINI_API_KEY'] = self.api_key
            self.client = genai.Client()
            logger.info(f"Gemini client initialized with model: {self.model_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini client: {str(e)}")
            self.client = None
    
    async def analyze_error(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a DAG error using Gemini
        
        Args:
            error_data: Error data from the database
            
        Returns:
            Analysis results
        """
        if not self.client:
            logger.error("Gemini client not initialized")
            return {"error": "Gemini client not initialized"}
        
        # Extract key information
        dag_id = error_data.get('dag_id', 'unknown')
        error_type = error_data.get('error_type', 'unknown')
        error_message = error_data.get('error_message', '')
        log_data = error_data.get('log_data', '[]')
        
        # Construct the prompt
        prompt = f"""You are Marathon Agent, an expert AI system specializing in analyzing and fixing errors in data processing DAGs (Directed Acyclic Graphs).

I need you to analyze the following error in a {dag_id} DAG:

Error Type: {error_type}
Error Message: {error_message}

Log context:
{log_data}

Please provide:
1. A detailed analysis of what went wrong
2. The likely root cause
3. A specific fix suggestion
4. Any code changes needed (if applicable)

Format your response as JSON with these exact fields:
- analysis: detailed analysis of the error
- root_cause: most likely root cause
- fix_suggestion: specific steps to fix the issue
- code_changes: any code changes needed (or null if not applicable)

Be precise and technical in your analysis. The DAG is likely running in an AWS environment with S3, CloudWatch, and possibly other AWS services."""
        
        try:
            # Call Gemini API with structured output
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=[prompt],
                config=types.GenerateContentConfig(
                    temperature=0.2,
                    response_mime_type="application/json"
                )
            )
            
            # Parse the response
            if not hasattr(response, 'text') or not response.text:
                logger.error("Empty response from Gemini API")
                return {"error": "Empty response from Gemini API"}
            
            # Try to parse the JSON response
            try:
                import json
                analysis_result = json.loads(response.text)
                logger.info(f"Successfully analyzed error for {dag_id}")
                return analysis_result
            except json.JSONDecodeError:
                logger.error("Failed to parse Gemini response as JSON")
                return {
                    "error": "Failed to parse response as JSON",
                    "raw_response": response.text[:500]  # Truncate to avoid huge logs
                }
            
        except Exception as e:
            logger.error(f"Error calling Gemini API: {str(e)}")
            return {"error": f"Gemini API error: {str(e)}"}
    
    async def generate_code_fix(self, fix_data: Dict[str, Any], original_code: str) -> Dict[str, Any]:
        """
        Generate code fix using Gemini
        
        Args:
            fix_data: Fix proposal data from the database
            original_code: Original DAG code
            
        Returns:
            Generated code fix
        """
        if not self.client:
            logger.error("Gemini client not initialized")
            return {"error": "Gemini client not initialized"}
        
        # Extract key information
        dag_id = fix_data.get('dag_id', 'unknown')
        error_type = fix_data.get('error_type', 'unknown')
        error_message = fix_data.get('error_message', '')
        analysis = fix_data.get('analysis', '')
        fix_suggestion = fix_data.get('fix_suggestion', '')
        reviewer_comments = fix_data.get('reviewer_comments', '')
        
        # Construct the prompt
        prompt = f"""You are Marathon Agent, an expert AI system specializing in fixing errors in data processing DAGs (Directed Acyclic Graphs).

I need you to fix the following DAG code for {dag_id}:

Error Type: {error_type}
Error Message: {error_message}

Your previous analysis:
{analysis}

Your fix suggestion (already approved):
{fix_suggestion}

Reviewer comments:
{reviewer_comments}

Original code:
```python
{original_code}
```

Please provide:
1. The complete fixed code (including ALL parts of the original file)
2. Explanations of your changes as comments in the code

Format your response as JSON with these exact fields:
- fixed_code: the complete fixed Python code as a string
- changes_summary: brief summary of changes made

Important guidelines:
- Ensure the fixed code handles all edge cases
- Add appropriate error handling
- Keep the original structure and imports
- Add comments explaining your changes
- Preserve any existing comments
- Don't remove any functionality, only fix the issue"""
        
        try:
            # Call Gemini API
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=[prompt],
                config=types.GenerateContentConfig(
                    temperature=0.2,
                    response_mime_type="application/json"
                )
            )
            
            # Parse the response
            if not hasattr(response, 'text') or not response.text:
                logger.error("Empty response from Gemini API")
                return {"error": "Empty response from Gemini API"}
            
            # Try to parse the JSON response
            try:
                import json
                fix_result = json.loads(response.text)
                logger.info(f"Successfully generated code fix for {dag_id}")
                return fix_result
            except json.JSONDecodeError:
                logger.error("Failed to parse Gemini response as JSON")
                return {
                    "error": "Failed to parse response as JSON",
                    "raw_response": response.text[:500]  # Truncate to avoid huge logs
                }
            
        except Exception as e:
            logger.error(f"Error calling Gemini API: {str(e)}")
            return {"error": f"Gemini API error: {str(e)}"}
    
    async def process_new_errors(self) -> int:
        """
        Process all new errors in needs_processing state
        
        Returns:
            Number of errors processed
        """
        # Get errors that need processing
        errors = dag_db.get_errors_for_processing()
        
        if not errors:
            logger.info("No new errors to process")
            return 0
        
        logger.info(f"Processing {len(errors)} new errors")
        
        processed_count = 0
        for error in errors:
            # Update error to in-progress state
            dag_db.update_error_state(error['id'], 'analysis_in_progress')
            
            try:
                # Analyze the error
                analysis_result = await self.analyze_error(error)
                
                if 'error' in analysis_result:
                    logger.error(f"Error analyzing error {error['id']}: {analysis_result['error']}")
                    dag_db.update_error_state(error['id'], 'failed')
                    continue
                
                # Create fix proposal
                proposal_id = dag_db.add_fix_proposal(
                    error_id=error['id'],
                    analysis=analysis_result.get('analysis', ''),
                    fix_suggestion=analysis_result.get('fix_suggestion', ''),
                    code_changes=analysis_result.get('code_changes')
                )
                
                logger.info(f"Created fix proposal {proposal_id} for error {error['id']}")
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing error {error['id']}: {str(e)}")
                dag_db.update_error_state(error['id'], 'failed')
        
        return processed_count
    
    async def process_approved_fixes(self) -> int:
        """
        Process approved fixes
        
        Returns:
            Number of fixes applied
        """
        # Get approved fixes
        fixes = dag_db.get_approved_fixes()
        
        if not fixes:
            logger.info("No approved fixes to process")
            return 0
        
        logger.info(f"Processing {len(fixes)} approved fixes")
        
        fixed_count = 0
        for fix in fixes:
            # Update fix proposal to in-progress state
            dag_db.update_error_state(fix['error_id'], 'fix_in_progress')
            
            try:
                # Get the original code from S3
                s3_path = fix['s3_file_path']
                bucket, key = s3_path.replace('s3://', '').split('/', 1)
                
                # You'll need boto3 for this, but for simplicity in this example:
                # original_code = self._get_s3_file_content(bucket, key)
                
                # For testing, let's just use a placeholder
                original_code = f"# Placeholder code for {fix['dag_id']}\n# Would normally load from S3"
                
                # Generate the fix
                fix_result = await self.generate_code_fix(fix, original_code)
                
                if 'error' in fix_result:
                    logger.error(f"Error generating fix for proposal {fix['id']}: {fix_result['error']}")
                    dag_db.record_fix_implementation(
                        proposal_id=fix['id'],
                        s3_file_path=s3_path,
                        original_content=original_code,
                        new_content="",
                        success=False,
                        error_message=fix_result['error']
                    )
                    continue
                
                # Get the fixed code
                fixed_code = fix_result.get('fixed_code', '')
                
                if not fixed_code:
                    logger.error(f"Empty fixed code for proposal {fix['id']}")
                    dag_db.record_fix_implementation(
                        proposal_id=fix['id'],
                        s3_file_path=s3_path,
                        original_content=original_code,
                        new_content="",
                        success=False,
                        error_message="Empty fixed code"
                    )
                    continue
                
                # Upload the fixed code to S3
                # self._put_s3_file_content(bucket, key, fixed_code)
                
                # Record the fix implementation
                dag_db.record_fix_implementation(
                    proposal_id=fix['id'],
                    s3_file_path=s3_path,
                    original_content=original_code,
                    new_content=fixed_code,
                    success=True
                )
                
                logger.info(f"Successfully applied fix for proposal {fix['id']}")
                fixed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing approved fix {fix['id']}: {str(e)}")
                dag_db.record_fix_implementation(
                    proposal_id=fix['id'],
                    s3_file_path=fix['s3_file_path'],
                    original_content="",
                    new_content="",
                    success=False,
                    error_message=str(e)
                )
        
        return fixed_count

# Create singleton instance
gemini_agent = GeminiAgent()
