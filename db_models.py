"""
Database models for Marathon Agent
"""
import os
import sqlite3
import json
import logging
import datetime
from typing import Dict, Any, List, Optional, Tuple

# Configure logging
logger = logging.getLogger("marathon-agent-db")
logger.setLevel(logging.INFO)

class DagDatabase:
    """SQLite database handler for DAG monitoring"""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize the database connection"""
        # Default to marathon_agent.db in backend directory
        self.db_path = db_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            'marathon_agent.db'
        )
        self._initialize_db()
    
    def _initialize_db(self):
        """Create tables if they don't exist"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create DAG errors table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS dag_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dag_id TEXT NOT NULL,
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                state TEXT NOT NULL,
                log_data TEXT,
                s3_file_path TEXT,
                detected_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # Create fix proposals table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS fix_proposals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                error_id INTEGER NOT NULL,
                analysis TEXT NOT NULL,
                fix_suggestion TEXT NOT NULL,
                code_changes TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (error_id) REFERENCES dag_errors (id)
            )
            ''')
            
            # Check if fix_approvals table exists
            cursor.execute('''
            SELECT name FROM sqlite_master WHERE type='table' AND name='fix_approvals';
            ''')
            
            if cursor.fetchone():
                # Check columns and add if missing
                required_columns = {
                    'reviewer': 'TEXT',
                    'comments': 'TEXT',
                    'approval_time': 'TEXT'
                }
                
                for column_name, column_type in required_columns.items():
                    try:
                        cursor.execute(f'SELECT {column_name} FROM fix_approvals LIMIT 1')
                    except sqlite3.OperationalError:
                        # Add column if it doesn't exist
                        logger.info(f"Adding {column_name} column to fix_approvals table")
                        cursor.execute(f'ALTER TABLE fix_approvals ADD COLUMN {column_name} {column_type};')
                        conn.commit()
            else:
                # Create approvals table if it doesn't exist
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS fix_approvals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    proposal_id INTEGER NOT NULL,
                    reviewer TEXT,
                    comments TEXT,
                    approved BOOLEAN NOT NULL,
                    approval_time TEXT,
                    FOREIGN KEY (proposal_id) REFERENCES fix_proposals (id)
                )
                ''')
            
            conn.commit()
        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
    
    def add_dag_error(self, dag_id: str, error_type: str, error_message: str, 
                      log_data: str = None, s3_file_path: str = None) -> int:
        """Add a new DAG error to the database"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get current timestamp
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            cursor.execute('''
            INSERT INTO dag_errors (dag_id, error_type, error_message, state, log_data, s3_file_path, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (dag_id, error_type, error_message, 'needs_processing', log_data, s3_file_path, current_time))
            
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Error adding DAG error: {str(e)}")
            if conn:
                conn.rollback()
            return -1
        finally:
            if conn:
                conn.close()
    
    def add_fix_proposal(self, error_id: int, analysis: str, 
                        fix_suggestion: str, code_changes: str) -> int:
        """Add a new fix proposal for a DAG error"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if the error exists
            cursor.execute('SELECT id FROM dag_errors WHERE id = ?', (error_id,))
            if not cursor.fetchone():
                logger.error(f"Error ID {error_id} not found")
                return -1
                
            # Get current timestamp
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add the fix proposal
            cursor.execute('''
            INSERT INTO fix_proposals (error_id, analysis, fix_suggestion, code_changes, state, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (error_id, analysis, fix_suggestion, code_changes, 'awaiting_approval', current_time))
            
            # Update the error state
            cursor.execute('''
            UPDATE dag_errors
            SET state = ?
            WHERE id = ?
            ''', ('awaiting_approval', error_id))
            
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Error adding fix proposal: {str(e)}")
            if conn:
                conn.rollback()
            return -1
        finally:
            if conn:
                conn.close()
    
    def approve_fix(self, proposal_id: int, reviewer: str = '', comments: str = '') -> bool:
        """Approve a fix proposal"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if the proposal exists
            cursor.execute('SELECT error_id FROM fix_proposals WHERE id = ?', (proposal_id,))
            result = cursor.fetchone()
            if not result:
                logger.error(f"Proposal ID {proposal_id} not found")
                return False
                
            error_id = result[0]
            
            # Get current timestamp
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add approval record
            cursor.execute('''
            INSERT INTO fix_approvals (proposal_id, reviewer, comments, approved, approval_time)
            VALUES (?, ?, ?, ?, ?)
            ''', (proposal_id, reviewer, comments, True, current_time))
            
            # Update proposal state
            cursor.execute('''
            UPDATE fix_proposals
            SET state = ?
            WHERE id = ?
            ''', ('approved_for_fix', proposal_id))
            
            # Update error state
            cursor.execute('''
            UPDATE dag_errors
            SET state = ?
            WHERE id = ?
            ''', ('approved_for_fix', error_id))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error approving fix: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def reject_fix(self, proposal_id: int, reviewer: str = '', comments: str = '') -> bool:
        """Reject a fix proposal"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if the proposal exists
            cursor.execute('SELECT error_id FROM fix_proposals WHERE id = ?', (proposal_id,))
            result = cursor.fetchone()
            if not result:
                logger.error(f"Proposal ID {proposal_id} not found")
                return False
                
            error_id = result[0]
            
            # Get current timestamp
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add rejection record
            cursor.execute('''
            INSERT INTO fix_approvals (proposal_id, reviewer, comments, approved, approval_time)
            VALUES (?, ?, ?, ?, ?)
            ''', (proposal_id, reviewer, comments, False, current_time))
            
            # Update proposal state
            cursor.execute('''
            UPDATE fix_proposals
            SET state = ?
            WHERE id = ?
            ''', ('rejected', proposal_id))
            
            # Update error state
            cursor.execute('''
            UPDATE dag_errors
            SET state = ?
            WHERE id = ?
            ''', ('needs_processing', error_id))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error rejecting fix: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def get_dag_errors(self, state: str = None, dag_id: str = None, limit: int = 100) -> List[Dict]:
        """Get DAG errors from the database"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = 'SELECT * FROM dag_errors'
            params = []
            
            # Add filters
            conditions = []
            if state:
                conditions.append('state = ?')
                params.append(state)
            
            if dag_id:
                conditions.append('dag_id = ?')
                params.append(dag_id)
                
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)
                
            query += ' ORDER BY detected_at DESC LIMIT ?'
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert rows to dictionaries
            errors = []
            for row in rows:
                error_dict = dict(row)
                if 'log_data' in error_dict and error_dict['log_data']:
                    try:
                        error_dict['log_details'] = json.loads(error_dict['log_data'])
                    except:
                        error_dict['log_details'] = {}
                errors.append(error_dict)
                
            return errors
        except Exception as e:
            logger.error(f"Error getting DAG errors: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_fix_proposals(self, state: str = None, error_id: str = None, limit: int = 100) -> List[Dict]:
        """Get fix proposals from the database"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Build the query
            query = '''
            SELECT fp.*, de.dag_id, de.error_type, de.error_message, de.log_data
            FROM fix_proposals fp
            JOIN dag_errors de ON fp.error_id = de.id
            '''
            params = []
            
            # Add filters
            conditions = []
            if state:
                conditions.append('fp.state = ?')
                params.append(state)
            
            if error_id:
                conditions.append('fp.error_id = ?')
                params.append(error_id)
                
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)
                
            query += ' ORDER BY fp.created_at DESC LIMIT ?'
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert rows to dictionaries
            proposals = []
            for row in rows:
                proposal_dict = dict(row)
                if 'log_data' in proposal_dict and proposal_dict['log_data']:
                    try:
                        log_details = json.loads(proposal_dict['log_data'])
                        proposal_dict['code_context'] = log_details.get('code_context', '')
                    except:
                        pass
                proposals.append(proposal_dict)
                
            return proposals
        except Exception as e:
            logger.error(f"Error getting fix proposals: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()
                
    def get_errors_for_processing(self, limit: int = 10) -> List[Dict]:
        """Get errors that need processing"""
        return self.get_dag_errors(state='needs_processing', limit=limit)
    
    def recreate_tables(self):
        """Drop and recreate all tables (for testing only)"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Drop tables
            cursor.execute('DROP TABLE IF EXISTS fix_approvals')
            cursor.execute('DROP TABLE IF EXISTS fix_proposals')
            cursor.execute('DROP TABLE IF EXISTS dag_errors')
            
            conn.commit()
            
            # Initialize tables again
            self._initialize_db()
            
            return True
        except Exception as e:
            logger.error(f"Error recreating tables: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

# Create singleton instance
dag_db = DagDatabase()