"""
Local DAG analyzer for Marathon Agent
Analyzes DAG files directly without requiring CloudWatch logs
"""
import os
import re
import json
import logging
import ast
import time
import random
from typing import Dict, Any, List, Optional, Tuple

from .db_models import dag_db

# Configure logging
logger = logging.getLogger("marathon-agent-dag-analyzer")
logger.setLevel(logging.INFO)

class LocalDagAnalyzer:
    """Service to analyze DAGs locally for errors"""
    
    def __init__(self, dag_dir: Optional[str] = None):
        """Initialize the DAG analyzer"""
        # Default to the dags directory in project root
        self.dag_dir = dag_dir or os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'dags')
        self.monitored_dags = ['extraction_dag.py', 'transformation_dag.py', 'loading_dag.py']
    
    def scan_dags_for_issues(self) -> List[Dict[str, Any]]:
        """Scan DAG files for potential issues and vulnerabilities"""
        issues_found = []
        
        # Check if DAG directory exists
        if not os.path.exists(self.dag_dir):
            logger.error(f"DAG directory not found: {self.dag_dir}")
            return issues_found
        
        # Scan each DAG file
        for dag_file in self.monitored_dags:
            dag_path = os.path.join(self.dag_dir, dag_file)
            
            if not os.path.exists(dag_path):
                logger.warning(f"DAG file not found: {dag_path}")
                continue
                
            # Read the file content
            try:
                with open(dag_path, 'r') as f:
                    content = f.read()
                
                # Analyze the content for potential issues
                issues = self._analyze_dag_code(dag_file, content)
                issues_found.extend(issues)
                
            except Exception as e:
                logger.error(f"Error analyzing {dag_file}: {str(e)}")
        
        return issues_found
    
    def _analyze_dag_code(self, dag_file: str, content: str) -> List[Dict[str, Any]]:
        """Analyze DAG code for potential issues"""
        issues = []
        dag_id = os.path.splitext(dag_file)[0]
        
        # Define patterns to check for common issues
        patterns = [
            {
                'pattern': r'except\s+Exception\s+as\s+e:',
                'error_type': 'broad_exception',
                'error_message': 'Overly broad exception handling detected',
                'description': 'Using broad exception handling makes it difficult to diagnose specific issues',
                'severity': 'medium',
                'line_window': 3  # Lines to include before and after the match
            },
            {
                'pattern': r'random\.random\(\)\s*<\s*ERROR_PROBABILITY',
                'error_type': 'random_error',
                'error_message': 'Random error generation in production code',
                'description': 'Code contains deliberate error injection which should not be in production',
                'severity': 'high',
                'line_window': 5
            },
            {
                'pattern': r'time\.sleep\(',
                'error_type': 'blocking_operation',
                'error_message': 'Blocking sleep operation detected',
                'description': 'Using time.sleep() in a DAG can lead to performance issues',
                'severity': 'low',
                'line_window': 2
            },
            {
                'pattern': r'print\(',
                'error_type': 'debug_code',
                'error_message': 'Debug print statements in production code',
                'description': 'Debug print statements should be replaced with proper logging',
                'severity': 'low',
                'line_window': 1
            },
            {
                'pattern': r'ERROR_PROBABILITY\s*=\s*0\.3',
                'error_type': 'error_injection',
                'error_message': 'Error probability set too high',
                'description': 'The error probability is set to 30%, which is too high for production',
                'severity': 'high',
                'line_window': 0
            },
            {
                'pattern': r'if\s+random\.random\(\)',
                'error_type': 'non_deterministic',
                'error_message': 'Non-deterministic behavior detected',
                'description': 'Using random values for control flow creates unpredictable behavior',
                'severity': 'medium',
                'line_window': 4
            }
        ]
        
        # Extract line numbers
        lines = content.split('\n')
        
        # Check each pattern
        for pattern_info in patterns:
            pattern = pattern_info['pattern']
            for i, line in enumerate(lines):
                if re.search(pattern, line):
                    # Calculate the window of code to include
                    start_line = max(0, i - pattern_info['line_window'])
                    end_line = min(len(lines) - 1, i + pattern_info['line_window'])
                    code_context = '\n'.join(lines[start_line:end_line + 1])
                    
                    # Generate a unique ID for this issue based on file and line
                    issue_id = f"{dag_id}_{pattern_info['error_type']}_{i}"
                    
                    # Get suggested fix
                    suggested_fix, fixed_code = self._generate_fix(
                        pattern_info['error_type'],
                        code_context,
                        lines[i]
                    )
                    
                    # Create the issue object
                    issue = {
                        'dag_id': dag_id,
                        'file_path': os.path.join(self.dag_dir, dag_file),
                        'error_type': pattern_info['error_type'],
                        'error_message': pattern_info['error_message'],
                        'description': pattern_info['description'],
                        'severity': pattern_info['severity'],
                        'line_number': i + 1,
                        'code_context': code_context,
                        'suggested_fix': suggested_fix,
                        'fixed_code': fixed_code,
                        'original_line': lines[i]
                    }
                    
                    # Add to issues list
                    issues.append(issue)
        
        # Ensure we don't detect too many issues at once (limit to 2 per file)
        if len(issues) > 2:
            # Sort by severity and take top 2
            severity_rank = {'high': 0, 'medium': 1, 'low': 2}
            issues.sort(key=lambda x: severity_rank.get(x['severity'], 3))
            issues = issues[:2]
        
        return issues
    
    def _generate_fix(self, error_type: str, code_context: str, problem_line: str) -> Tuple[str, str]:
        """Generate a suggested fix for the issue"""
        if error_type == 'broad_exception':
            # Suggest catching specific exceptions
            suggested_fix = "Replace broad exception handling with specific exception types"
            fixed_code = code_context.replace(
                'except Exception as e:',
                'except (ValueError, TypeError, KeyError) as e:'
            )
            
        elif error_type == 'random_error':
            # Suggest removing random error injection
            suggested_fix = "Remove random error injection from production code"
            fixed_code = re.sub(
                r'if\s+random\.random\(\)\s*<\s*ERROR_PROBABILITY.*?:(.*?)(?=\n\s*\w|\Z)',
                '',
                code_context,
                flags=re.DOTALL
            )
            
        elif error_type == 'blocking_operation':
            # Suggest async alternative
            suggested_fix = "Replace blocking sleep with non-blocking async alternative"
            fixed_code = code_context.replace(
                'time.sleep(',
                'await asyncio.sleep('
            )
            if 'import asyncio' not in code_context:
                fixed_code = "import asyncio\n" + fixed_code
                
        elif error_type == 'debug_code':
            # Suggest using logger
            suggested_fix = "Replace print statements with proper logging"
            fixed_code = code_context.replace(
                'print(',
                'logger.debug('
            )
            
        elif error_type == 'error_injection':
            # Suggest lowering error probability
            suggested_fix = "Reduce error probability for production environment"
            fixed_code = code_context.replace(
                'ERROR_PROBABILITY = 0.3',
                'ERROR_PROBABILITY = 0.01  # Reduced for production'
            )
            
        elif error_type == 'non_deterministic':
            # Suggest deterministic alternative
            suggested_fix = "Replace non-deterministic behavior with environment-based control"
            fixed_code = re.sub(
                r'if\s+random\.random\(\)\s*<.*?:',
                'if os.getenv("ENABLE_FEATURE", "false").lower() == "true":',
                code_context
            )
            
        else:
            # Generic fix suggestion
            suggested_fix = "Review and fix the highlighted code"
            fixed_code = code_context
            
        return suggested_fix, fixed_code
    
    def register_detected_issues(self) -> List[int]:
        """Register detected issues in the database"""
        issues = self.scan_dags_for_issues()
        error_ids = []
        
        for issue in issues:
            # Register the error in the database
            error_id = dag_db.add_dag_error(
                dag_id=issue['dag_id'],
                error_type=issue['error_type'],
                error_message=issue['error_message'],
                log_data=json.dumps({
                    'description': issue['description'],
                    'line_number': issue['line_number'],
                    'code_context': issue['code_context'],
                    'suggested_fix': issue['suggested_fix'],
                    'fixed_code': issue['fixed_code'],
                    'severity': issue['severity']
                }),
                s3_file_path=issue['file_path']
            )
            
            error_ids.append(error_id)
            logger.info(f"Registered error {error_id} for {issue['dag_id']}: {issue['error_type']}")
        
        return error_ids

# Create singleton instance
dag_analyzer = LocalDagAnalyzer()
