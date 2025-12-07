"""
Database schema for the Marathon Agent application
"""
import sqlite3
import json
import os
import time
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger("marathon-agent-db")

class DatabaseManager:
    def __init__(self, db_path: str):
        """Initialize the database manager with the given path"""
        self.db_path = db_path
        self._create_tables()
    
    def _create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tasks table - stores basic task information
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                workflow_name TEXT NOT NULL,
                workflow_version TEXT NOT NULL,
                current_state TEXT NOT NULL,
                completion_state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                creator_id TEXT NOT NULL,
                priority TEXT DEFAULT 'normal',
                max_retries INTEGER DEFAULT 3,
                retry_count INTEGER DEFAULT 0,
                timeout_seconds INTEGER DEFAULT 120,
                is_complete BOOLEAN DEFAULT 0,
                has_error BOOLEAN DEFAULT 0
            )
            ''')
            
            # Task States table - stores state transition history
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                state_name TEXT NOT NULL,
                entered_at TEXT NOT NULL,
                exited_at TEXT,
                duration_seconds INTEGER,
                result TEXT,
                has_error BOOLEAN DEFAULT 0,
                error_message TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            )
            ''')
            
            # Task Data table - stores JSON data associated with tasks
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_data (
                task_id TEXT PRIMARY KEY,
                prompt TEXT NOT NULL,
                context TEXT,
                parameters TEXT,
                results TEXT,
                next_states TEXT,
                error_details TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            )
            ''')
            
            # Create indexes for faster lookups
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_task_states_task_id ON task_states(task_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_workflow_id ON tasks(workflow_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_current_state ON tasks(current_state)')
            
            conn.commit()
            conn.close()
            logger.info(f"Database tables created in {self.db_path}")
        except Exception as e:
            logger.error(f"Error creating database tables: {str(e)}")
            raise
    
    def create_task(self, task_data: Dict[str, Any]) -> str:
        """
        Create a new task in the database
        Returns the task_id
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            task_id = task_data.get('task_id')
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Insert into tasks table
            cursor.execute('''
            INSERT INTO tasks (
                task_id, workflow_id, workflow_name, workflow_version,
                current_state, completion_state, created_at, updated_at,
                creator_id, priority, max_retries, timeout_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                task_id,
                task_data.get('workflow_id'),
                task_data.get('workflow', {}).get('name', 'unknown'),
                task_data.get('workflow', {}).get('version', '1.0'),
                task_data.get('state'),
                task_data.get('workflow', {}).get('completion_state', 'completion'),
                now,
                now,
                task_data.get('metadata', {}).get('creator_id', 'system'),
                task_data.get('metadata', {}).get('priority', 'normal'),
                task_data.get('metadata', {}).get('max_retries', 3),
                task_data.get('metadata', {}).get('timeout_seconds', 120)
            ))
            
            # Insert initial state into task_states table
            cursor.execute('''
            INSERT INTO task_states (
                task_id, state_name, entered_at
            ) VALUES (?, ?, ?)
            ''', (
                task_id,
                task_data.get('state'),
                now
            ))
            
            # Insert into task_data table
            data = task_data.get('data', {})
            workflow = task_data.get('workflow', {})
            error = task_data.get('error', {})
            
            cursor.execute('''
            INSERT INTO task_data (
                task_id, prompt, context, parameters, results, next_states, error_details
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                task_id,
                data.get('prompt', ''),
                data.get('context', ''),
                json.dumps(data.get('parameters', {})),
                json.dumps(data.get('results', {})),
                json.dumps(workflow.get('next_states', [])),
                json.dumps(error) if error and error.get('has_error') else None
            ))
            
            conn.commit()
            conn.close()
            logger.info(f"Created new task with ID: {task_id}")
            return task_id
        except Exception as e:
            logger.error(f"Error creating task: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            raise
    
    def update_task_state(self, task_id: str, new_state: str, result: Optional[str] = None, 
                         error: Optional[Dict[str, Any]] = None) -> bool:
        """
        Update a task's state and add a new state entry
        Returns True if successful, False otherwise
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get current state
            cursor.execute('SELECT current_state, completion_state FROM tasks WHERE task_id = ?', (task_id,))
            row = cursor.fetchone()
            if not row:
                logger.error(f"Task not found: {task_id}")
                conn.close()
                return False
                
            current_state, completion_state = row
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Update the exit time for the previous state
            cursor.execute('''
            UPDATE task_states 
            SET exited_at = ?, 
                duration_seconds = ROUND((strftime('%s', ?) - strftime('%s', entered_at))),
                result = ?
            WHERE task_id = ? AND state_name = ? AND exited_at IS NULL
            ''', (now, now, result, task_id, current_state))
            
            # Add new state entry
            cursor.execute('''
            INSERT INTO task_states (
                task_id, state_name, entered_at, has_error, error_message
            ) VALUES (?, ?, ?, ?, ?)
            ''', (
                task_id,
                new_state,
                now,
                1 if error else 0,
                error.get('message') if error else None
            ))
            
            # Check if this is the completion state
            is_complete = (new_state == completion_state)
            has_error = bool(error)
            
            # Update the task record
            cursor.execute('''
            UPDATE tasks 
            SET current_state = ?,
                updated_at = ?,
                is_complete = ?,
                has_error = ?
            WHERE task_id = ?
            ''', (new_state, now, is_complete, has_error, task_id))
            
            # If there's an error, update the error details
            if error:
                cursor.execute('''
                UPDATE task_data 
                SET error_details = ?
                WHERE task_id = ?
                ''', (json.dumps(error), task_id))
            
            # Update results in task_data
            cursor.execute('SELECT results FROM task_data WHERE task_id = ?', (task_id,))
            results_json = cursor.fetchone()[0]
            results = json.loads(results_json) if results_json else {}
            
            if result:
                results[current_state] = result
                
            cursor.execute('''
            UPDATE task_data 
            SET results = ?
            WHERE task_id = ?
            ''', (json.dumps(results), task_id))
            
            conn.commit()
            conn.close()
            logger.info(f"Updated task {task_id} state from {current_state} to {new_state}")
            return True
        except Exception as e:
            logger.error(f"Error updating task state: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            raise
    
    def get_task(self, task_id: str) -> Dict[str, Any]:
        """Get full task details including state history"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get task details
            cursor.execute('''
            SELECT t.*, td.prompt, td.context, td.parameters, td.results, td.next_states, td.error_details
            FROM tasks t
            JOIN task_data td ON t.task_id = td.task_id
            WHERE t.task_id = ?
            ''', (task_id,))
            
            task_row = cursor.fetchone()
            if not task_row:
                conn.close()
                return {}
                
            # Convert row to dict
            task = dict(task_row)
            
            # Parse JSON fields
            task['parameters'] = json.loads(task['parameters']) if task['parameters'] else {}
            task['results'] = json.loads(task['results']) if task['results'] else {}
            task['next_states'] = json.loads(task['next_states']) if task['next_states'] else []
            task['error_details'] = json.loads(task['error_details']) if task['error_details'] else {}
            
            # Get state history
            cursor.execute('''
            SELECT state_name, entered_at, exited_at, duration_seconds, result, has_error, error_message
            FROM task_states
            WHERE task_id = ?
            ORDER BY entered_at
            ''', (task_id,))
            
            state_rows = cursor.fetchall()
            task['state_history'] = [dict(row) for row in state_rows]
            
            conn.close()
            return task
        except Exception as e:
            logger.error(f"Error getting task: {str(e)}")
            if conn:
                conn.close()
            return {}
    
    def list_tasks(self, workflow_id: Optional[str] = None, 
                  state: Optional[str] = None,
                  limit: int = 100) -> List[Dict[str, Any]]:
        """List tasks with optional filtering"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = "SELECT task_id, workflow_id, workflow_name, current_state, created_at, updated_at, is_complete, has_error FROM tasks"
            params = []
            
            # Apply filters
            conditions = []
            if workflow_id:
                conditions.append("workflow_id = ?")
                params.append(workflow_id)
                
            if state:
                conditions.append("current_state = ?")
                params.append(state)
                
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
                
            query += " ORDER BY updated_at DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(query, params)
            
            rows = cursor.fetchall()
            tasks = [dict(row) for row in rows]
            
            conn.close()
            return tasks
        except Exception as e:
            logger.error(f"Error listing tasks: {str(e)}")
            if conn:
                conn.close()
            return []

# Initialize the database manager when imported
db_manager = None

def init_db(db_path: str):
    """Initialize the database manager"""
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager(db_path)
    return db_manager

def get_db_manager():
    """Get the database manager instance"""
    global db_manager
    if db_manager is None:
        # Default to SQLite DB path from config or environment
        db_path = os.getenv("SQLITE_DB", "marathon_agent.db")
        db_manager = DatabaseManager(db_path)
    return db_manager
