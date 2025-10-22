import signal
import sys
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import pandas as pd
import psycopg2
import io
import os
import re
import csv
from datetime import datetime
from difflib import SequenceMatcher
import hashlib
from werkzeug.utils import secure_filename
import logging
import uuid
from datetime import datetime
from logging.config import dictConfig
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import json

# Configure logging BEFORE creating the Flask app
dictConfig({
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        },
        'detailed': {
            'format': '[%(asctime)s] %(levelname)s %(name)s %(threadName)s : %(message)s'
        }
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'flask_import.log',
            'formatter': 'detailed',
            'level': 'INFO',
        },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'level': 'DEBUG',
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file']
    },
    'loggers': {
        'werkzeug': {
            'level': 'INFO',
            'handlers': ['console', 'file'],
            'propagate': False
        }
    }
})

app = Flask(__name__)
app.secret_key = "secret_key_123"
app.config['SESSION_COOKIE_NAME'] = 'import_system_session'
app.config['PERMANENT_SESSION_LIFETIME'] = 3600  # 1 hour
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = False  # Set to True in production with HTTPS

# ---------- APPLICATION CONFIG ----------
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB max file size
app.config['UPLOAD_EXTENSIONS'] = ['.csv', '.xlsx', '.xls']
app.config['UPLOAD_FOLDER'] = 'uploads'

# ---------- DATABASE CONFIG ----------
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres", 
    "user": "postgres",
    "password": "123456",
    "connect_timeout": 10,
    "options": "-c statement_timeout=120000 -c lock_timeout=120000"
}

def get_db_connection():
    """Enhanced database connection with timeout settings"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 120000;")
            cur.execute("SET lock_timeout = 120000;")
        return conn
    except Exception as e:
        app.logger.error(f"Database connection failed: {e}")
        raise

# ---------- GRACEFUL SHUTDOWN HANDLER ----------
def graceful_shutdown(signum=None, frame=None):
    """Handle graceful shutdown - SIMPLIFIED"""
    app.logger.info("Received shutdown signal, initiating shutdown...")
    
    # Stop accepting new tasks
    try:
        progress_tracker.executor.shutdown(wait=False)
        app.logger.info("Thread pool executor shutdown initiated")
    except Exception as e:
        app.logger.warning(f"Error shutting down executor: {e}")
    
    # Force exit after short delay
    def force_exit():
        time.sleep(2)
        os._exit(0)
    
    exit_thread = threading.Thread(target=force_exit, daemon=True)
    exit_thread.start()

# ---------- REQUEST DEDUPLICATION ----------
class RequestDeduplication:
    def __init__(self):
        self.active_requests = {}
        self.request_timeout = 600  # 10 minutes
    
    def generate_request_fingerprint(self, request):
        """Generate unique fingerprint for each upload request"""
        fingerprint_data = {
            'user': session.get('user', 'anonymous'),
            'platform': request.form.get('platform', ''),
            'files': []
        }
        
        for file_field in ['b2b_file', 'b2c_file', 'payout_file']:
            file = request.files.get(file_field)
            if file and file.filename:
                file_info = {
                    'field': file_field,
                    'filename': file.filename,
                    'content_type': file.content_type
                }
                fingerprint_data['files'].append(file_info)
        
        fingerprint_str = str(sorted(fingerprint_data.items()))
        return hashlib.md5(fingerprint_str.encode()).hexdigest()[:16]
    
    def is_duplicate_request(self, fingerprint):
        """Check if this is a duplicate request"""
        now = datetime.now()
        self.cleanup_expired_requests(now)
        
        if fingerprint in self.active_requests:
            app.logger.warning(f"Duplicate request detected: {fingerprint}")
            return True
        
        self.active_requests[fingerprint] = now
        app.logger.info(f"New request tracking: {fingerprint}")
        return False
    
    def cleanup_expired_requests(self, current_time=None):
        """Remove expired requests from tracking"""
        if current_time is None:
            current_time = datetime.now()
        
        expired_requests = []
        for fingerprint, timestamp in self.active_requests.items():
            if (current_time - timestamp).total_seconds() > self.request_timeout:
                expired_requests.append(fingerprint)
        
        for fingerprint in expired_requests:
            del self.active_requests[fingerprint]
            app.logger.info(f"Removed expired request: {fingerprint}")
    
    def complete_request(self, fingerprint):
        """Mark request as completed"""
        if fingerprint in self.active_requests:
            del self.active_requests[fingerprint]
            app.logger.info(f"Request completed: {fingerprint}")

request_deduplicator = RequestDeduplication()

# ---------- PROGRESS TRACKER ----------
class ProgressTracker:
    def __init__(self):
        self.active_imports = {}
        self.executor = ThreadPoolExecutor(max_workers=3)
    
    def start_import_process(self, import_batch_id, files_to_process, platform):
        """Start the automated import process"""
        self.active_imports[import_batch_id] = {
            'status': 'starting',
            'current_step': 'Initializing import process...',
            'progress': 0,
            'files': files_to_process,
            'platform': platform,
            'start_time': datetime.now(),
            'logs': [],
            'results': [],
            'uploaded_months': set(),
            'file_types': set()
        }
        
        # Extract months and file types
        uploaded_months = self._extract_months_from_files(files_to_process)
        file_types = {file_info['type'] for file_info in files_to_process}
        
        self.active_imports[import_batch_id]['uploaded_months'] = uploaded_months
        self.active_imports[import_batch_id]['file_types'] = file_types
        
        # Start background processing
        future = self.executor.submit(self._run_automated_import, import_batch_id, files_to_process, platform)
        future.add_done_callback(lambda x: self._cleanup_import(import_batch_id))
        
        return import_batch_id

    def _run_automated_import(self, import_batch_id, files_to_process, platform):
        """Run the complete automated import process"""
        try:
            # STEP 1: File upload and staging
            self._update_progress(import_batch_id, 'Uploading files to staging tables...', 10)
            staging_results = self._process_files_to_staging(import_batch_id, files_to_process, platform)
            
            if not staging_results['success']:
                self._update_progress(import_batch_id, f'Staging failed: {staging_results["error"]}', 100, 'error')
                return

            # STEP 2: Data processing to final tables
            self._update_progress(import_batch_id, 'Processing data in final tables...', 40)
            processing_results = self._process_final_tables(import_batch_id, staging_results['file_types'], platform)
            
            if not processing_results['success']:
                self._update_progress(import_batch_id, f'Processing failed: {processing_results["error"]}', 100, 'error')
                return

            # STEP 3: Smart Reconciliation
            uploaded_months = self.active_imports[import_batch_id].get('uploaded_months', [])
            file_types = self.active_imports[import_batch_id].get('file_types', set())
            
            if uploaded_months:
                self._update_progress(import_batch_id, 'Checking if reconciliation is needed...', 65)
                self._add_log(import_batch_id, f'Uploaded files for months: {uploaded_months}', 'info')
                
                # Check for reconciliation
                should_reconcile, reconcile_message = self._should_run_reconciliation(uploaded_months, file_types)
                
                if should_reconcile:
                    self._add_log(import_batch_id, f'Running reconciliation for: {reconcile_message}', 'info')
                    
                    for month_year in reconcile_message:
                        self._update_progress(import_batch_id, f'Running reconciliation for {month_year}...', 70)
                        recon_results = self._run_reconciliation_for_month(month_year, import_batch_id)
                        
                        if not recon_results['success']:
                            self._add_log(import_batch_id, f'Reconciliation failed for {month_year}: {recon_results["error"]}', 'warning')
                        else:
                            self._add_log(import_batch_id, f'Reconciliation completed for {month_year}', 'success')
                else:
                    self._add_log(import_batch_id, f'Skipping reconciliation: {reconcile_message}', 'info')
            
            # STEP 4: Completion
            self._update_progress(import_batch_id, 'Import completed successfully!', 100, 'success')
            self._add_log(import_batch_id, 'All import procedures completed successfully', 'success')
            
        except Exception as e:
            error_msg = f'Import process failed: {str(e)}'
            self._update_progress(import_batch_id, error_msg, 100, 'error')
            self._add_log(import_batch_id, error_msg, 'error')
            app.logger.error(f"[{import_batch_id}] {error_msg}")

    def _should_run_reconciliation(self, uploaded_months, file_types):
        """Check if reconciliation should run"""
        has_sales = any(ft in file_types for ft in ['b2b', 'b2c'])
        has_payout = 'payout' in file_types
        
        if not (has_sales and has_payout):
            return False, f"Missing files for reconciliation. Sales: {has_sales}, Payout: {has_payout}"
        
        reconcilable_months = self._find_reconcilable_months_for_uploaded(uploaded_months)
        if not reconcilable_months:
            return False, "No months with complete data found"
        
        return True, reconcilable_months

    def _find_reconcilable_months_for_uploaded(self, uploaded_months):
        """Find which uploaded months have complete data for reconciliation"""
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                reconcilable_months = []
                
                for month_year in uploaded_months:
                    year, month = month_year.split('-')
                    
                    cur.execute("""
                        SELECT 
                            EXISTS (SELECT 1 FROM spigen.stg_amz_b2b WHERE EXTRACT(YEAR FROM invoice_date) = %s AND EXTRACT(MONTH FROM invoice_date) = %s LIMIT 1) as has_b2b,
                            EXISTS (SELECT 1 FROM spigen.stg_amz_b2c WHERE EXTRACT(YEAR FROM invoice_date) = %s AND EXTRACT(MONTH FROM invoice_date) = %s LIMIT 1) as has_b2c,
                            EXISTS (SELECT 1 FROM spigen.stg_amz_payout WHERE EXTRACT(YEAR FROM date_time) = %s AND EXTRACT(MONTH FROM date_time) = %s LIMIT 1) as has_payout
                    """, (int(year), int(month), int(year), int(month), int(year), int(month)))
                    
                    result = cur.fetchone()
                    if result and all(result):
                        reconcilable_months.append(month_year)
                        app.logger.info(f"Month {month_year} has complete data for reconciliation")
                
                return reconcilable_months
                
        except Exception as e:
            app.logger.error(f"Error finding reconcilable months: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def _extract_months_from_files(self, files_to_process):
        """Extract month-year from uploaded files"""
        months = set()
        
        for file_info in files_to_process:
            filename = file_info['filename'].upper()
            
            # Try to extract from filename first
            month_from_filename = self._extract_month_from_filename(filename, file_info['type'])
            if month_from_filename:
                months.add(month_from_filename)
                continue
            
            # If filename extraction fails, try to extract from data
            try:
                month_from_data = self._extract_month_from_file_data(file_info['file_path'], file_info['type'])
                if month_from_data:
                    months.add(month_from_data)
            except Exception as e:
                app.logger.warning(f"Could not extract month from data for {filename}: {e}")
        
        return list(months)

    def _extract_month_from_filename(self, filename, file_type):
        """Extract month from filename with pattern matching"""
        month_map = {
            'JANUARY': '01', 'FEBRUARY': '02', 'MARCH': '03', 'APRIL': '04',
            'MAY': '05', 'JUNE': '06', 'JULY': '07', 'AUGUST': '08',
            'SEPTEMBER': '09', 'OCTOBER': '10', 'NOVEMBER': '11', 'DECEMBER': '12'
        }
        
        month_abbr_map = {
            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
            'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
        }
        
        # Pattern 1: MTR_B2B-APRIL-2024.csv or MTR_B2C-MAY-2024.csv
        b2b_pattern = r'MTR_B2B-([A-Z]+)-(\d{4})'
        b2c_pattern = r'MTR_B2C-([A-Z]+)-(\d{4})'
        
        for pattern in [b2b_pattern, b2c_pattern]:
            match = re.search(pattern, filename)
            if match:
                month_name, year = match.groups()
                if month_name in month_map:
                    return f"{year}-{month_map[month_name]}"
        
        return None

    def _extract_month_from_file_data(self, file_path, file_type):
        """Extract month from file data as fallback"""
        try:
            if file_type == 'payout':
                df = pd.read_csv(file_path, usecols=['date/time'], nrows=100)
                date_col = 'date/time'
            else:
                df = pd.read_csv(file_path, usecols=['invoice_date'], nrows=100)
                date_col = 'invoice_date'
            
            if date_col in df.columns:
                dates = optimized_date_parser(df[date_col].dropna())
                if not dates.empty:
                    month_years = dates.dt.strftime('%Y-%m').value_counts()
                    if not month_years.empty:
                        return month_years.index[0]
        except Exception as e:
            app.logger.warning(f"Failed to extract month from data: {e}")
        
        return None

    def _run_reconciliation_for_month(self, month_year, import_batch_id):
        """Run reconciliation for a specific month-year"""
        try:
            app.logger.info(f"Running reconciliation for {month_year}")
            
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("CALL spigen.process_reconciliation_opt()")
                conn.commit()
            
            app.logger.info(f"Reconciliation completed successfully for {month_year}")
            return {'success': True}
                
        except Exception as e:
            error_msg = f"Reconciliation failed for {month_year}: {str(e)}"
            app.logger.error(f"[{import_batch_id}] {error_msg}")
            return {'success': False, 'error': error_msg}
        finally:
            if conn:
                conn.close()
        
    def _process_files_to_staging(self, import_batch_id, files_to_process, platform):
        """Process files to staging tables"""
        try:
            processed_types = set()
            
            for file_info in files_to_process:
                file_type = file_info['type']
                file_path = file_info['file_path']
                
                self._update_progress(import_batch_id, f'Importing {file_type.upper()} to staging...', 20)
                self._add_log(import_batch_id, f'Starting {file_type.upper()} import', 'info')
                
                # Process file using the optimized function
                file_results = process_single_file_optimized(file_path, platform, file_type, import_batch_id)
                
                # Check results
                for result in file_results:
                    if 'SUCCESS' in result:
                        processed_types.add('sales' if file_type in ['b2b', 'b2c'] else 'payout')
                        processed_types.add(file_type)
                        self._add_log(import_batch_id, result, 'success')
                    elif 'ERROR' in result:
                        self._add_log(import_batch_id, result, 'error')
                        return {'success': False, 'error': result}
                    else:
                        self._add_log(import_batch_id, result, 'warning')
                
                time.sleep(1)
            
            return {'success': True, 'file_types': processed_types}
            
        except Exception as e:
            error_msg = f'Staging process failed: {str(e)}'
            self._add_log(import_batch_id, error_msg, 'error')
            return {'success': False, 'error': error_msg}
    
    def _process_final_tables(self, import_batch_id, file_types, platform):
        """Process data from staging to final tables"""
        try:
            self._update_progress(import_batch_id, 'Transferring data to final tables...', 50)
            self._add_log(import_batch_id, 'Starting data processing in final tables', 'info')
            
            conn = get_db_connection()
            with conn.cursor() as cur:
                if any(t in file_types for t in ['b2b', 'b2c', 'sales']):
                    self._add_log(import_batch_id, 'Processing sales data in final tables', 'info')
                    try:
                        cur.execute("CALL spigen.process_amazon_sales(%s)", (import_batch_id,))
                        self._add_log(import_batch_id, 'Sales data processed successfully', 'success')
                    except psycopg2.Error as e:
                        app.logger.warning(f"Sales procedure with batch_id failed, using fallback: {e}")
                        cur.execute("CALL spigen.process_amazon_sales()")
                        self._add_log(import_batch_id, 'Sales data processed (fallback mode)', 'warning')
                
                if 'payout' in file_types:
                    self._add_log(import_batch_id, 'Processing payout data in final tables', 'info')
                    try:
                        cur.execute("CALL spigen.process_amazon_payout(%s)", (import_batch_id,))
                        self._add_log(import_batch_id, 'Payout data processed successfully', 'success')
                    except psycopg2.Error as e:
                        app.logger.warning(f"Payout procedure with batch_id failed, using fallback: {e}")
                        cur.execute("CALL spigen.process_amazon_payout()")
                        self._add_log(import_batch_id, 'Payout data processed (fallback mode)', 'warning')
                
                conn.commit()
            
            self._update_progress(import_batch_id, 'Data processing completed', 60)
            self._add_log(import_batch_id, 'All data processing completed', 'success')
            return {'success': True}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
        finally:
            if conn:
                conn.close()
    
    def _update_progress(self, import_batch_id, step, progress, status='processing'):
        """Update progress for an import"""
        if import_batch_id in self.active_imports:
            self.active_imports[import_batch_id].update({
                'current_step': step,
                'progress': progress,
                'status': status,
                'last_update': datetime.now()
            })
    
    def _add_log(self, import_batch_id, message, level='info'):
        """Add log message"""
        if import_batch_id in self.active_imports:
            timestamp = datetime.now()
            log_entry = {
                'timestamp': timestamp,
                'message': message,
                'level': level
            }
            self.active_imports[import_batch_id]['logs'].append(log_entry)
            
            log_func = getattr(app.logger, level, app.logger.info)
            log_func(f"[{import_batch_id}] {message}")
    
    def _cleanup_import(self, import_batch_id):
        """Clean up completed imports after delay"""
        if import_batch_id in self.active_imports:
            def remove_later():
                time.sleep(60)
                if import_batch_id in self.active_imports:
                    import_data = self.active_imports[import_batch_id]
                    if import_data['status'] in ['success', 'error']:
                        del self.active_imports[import_batch_id]
                        app.logger.info(f"[{import_batch_id}] Cleaned up from progress tracker")
                
            threading.Thread(target=remove_later, daemon=True).start()

    def cleanup_stuck_imports(self):
        """Clean up imports that have been stuck for too long"""
        current_time = datetime.now()
        stuck_imports = []
        
        for import_id, import_data in self.active_imports.items():
            if 'last_update' in import_data:
                time_since_update = (current_time - import_data['last_update']).total_seconds()
                if time_since_update > 1800 and import_data['status'] not in ['success', 'error']:
                    stuck_imports.append(import_id)
        
        for import_id in stuck_imports:
            app.logger.warning(f"Cleaning up stuck import: {import_id}")
            del self.active_imports[import_id]
    
    def get_progress(self, import_batch_id):
        """Get current progress for an import"""
        return self.active_imports.get(import_batch_id, None)

# Initialize progress tracker
progress_tracker = ProgressTracker()

# ---------- OPTIMIZED FILE PROCESSING FUNCTION ----------
def process_single_file_optimized(file_path, platform, table_type, import_batch_id):
    """Optimized file processing with chunked database inserts"""
    table_name = f"stg_{platform.lower()}_{table_type.lower()}"
    results = []
    
    app.logger.info(f"[{import_batch_id}] Processing {table_type} file: {file_path}")
    
    try:
        # Read and process file
        if table_type.lower() == 'payout':
            df = smart_payout_processor(file_path)
        else:
            df = process_b2b_b2c_file(file_path)
        
        df = optimize_dataframe(df, table_name)

        if df.empty:
            result_msg = f"WARNING - {table_type.upper()}: No data found after processing"
            results.append(result_msg)
            return results

        # Database operations with chunked inserts
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Truncate table
                cur.execute(f"TRUNCATE TABLE spigen.{table_name}")
                app.logger.info(f"[{import_batch_id}] Truncated table: {table_name}")
                
                # Insert data in chunks
                total_rows = len(df)
                chunk_size = 500
                chunks = [df[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]
                
                app.logger.info(f"[{import_batch_id}] Inserting {len(chunks)} chunks")
                
                inserted_rows = 0
                for i, chunk in enumerate(chunks):
                    try:
                        if (i + 1) % 10 == 0:
                            app.logger.info(f"[{import_batch_id}] Inserting chunk {i+1}/{len(chunks)}")
                        
                        output = io.StringIO()
                        chunk.to_csv(output, index=False, header=False, na_rep='NULL')
                        output.seek(0)
                        
                        columns_str = ", ".join(df.columns)
                        copy_sql = f"COPY spigen.{table_name} ({columns_str}) FROM STDIN WITH CSV NULL 'NULL'"
                        cur.copy_expert(copy_sql, output)
                        inserted_rows += len(chunk)
                        
                    except Exception as chunk_error:
                        app.logger.error(f"[{import_batch_id}] Failed to insert chunk {i+1}: {str(chunk_error)}")
                        continue
                
                conn.commit()
                app.logger.info(f"[{import_batch_id}] Successfully inserted {inserted_rows} rows")

        except Exception as db_error:
            conn.rollback()
            raise db_error
        finally:
            conn.close()

        # Success
        result_msg = f"SUCCESS - {table_type.upper()}: {inserted_rows} records imported to staging"
        results.append(result_msg)
        app.logger.info(f"[{import_batch_id}] {result_msg}")

    except Exception as e:
        error_msg = f"ERROR - {table_type.upper()}: {str(e)}"
        results.append(error_msg)
        app.logger.error(f"[{import_batch_id}] {error_msg}")
        
    finally:
        # Cleanup temporary file
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                app.logger.info(f"[{import_batch_id}] Cleaned up temporary file")
            except Exception as e:
                app.logger.warning(f"[{import_batch_id}] Failed to cleanup temp file: {str(e)}")
    
    return results


## ---------- REQUEST DEDUPLICATION CONFIG ----------
class RequestDeduplication:
    def __init__(self):
        self.active_requests = {}
        self.cleanup_interval = 300  # 5 minutes
        self.request_timeout = 600   # 10 minutes
    
    def generate_request_fingerprint(self, request):
        """Generate unique fingerprint for each upload request - FIXED VERSION"""
        fingerprint_data = {
            'user': session.get('user', 'anonymous'),
            'platform': request.form.get('platform', ''),
            'files': []
        }
        
        # Add file fingerprints - FIX: Don't read file content, use metadata only
        for file_field in ['b2b_file', 'b2c_file', 'payout_file']:
            file = request.files.get(file_field)
            if file and file.filename:
                file_info = {
                    'field': file_field,
                    'filename': file.filename,
                    'content_type': file.content_type
                }
                fingerprint_data['files'].append(file_info)
        
        # Create hash fingerprint
        fingerprint_str = str(sorted(fingerprint_data.items()))
        return hashlib.md5(fingerprint_str.encode()).hexdigest()[:16]
    
    def is_duplicate_request(self, fingerprint):
        """Check if this is a duplicate request"""
        now = datetime.now()
        
        # Clean up old requests
        self.cleanup_expired_requests(now)
        
        if fingerprint in self.active_requests:
            app.logger.warning(f"[DUPLICATE] Request detected: {fingerprint}")
            return True
        
        # Mark request as active
        self.active_requests[fingerprint] = now
        app.logger.info(f"[NEW_REQUEST] Tracking request: {fingerprint}")
        return False
    
    def cleanup_expired_requests(self, current_time=None):
        """Remove expired requests from tracking"""
        if current_time is None:
            current_time = datetime.now()
        
        expired_requests = []
        for fingerprint, timestamp in self.active_requests.items():
            if (current_time - timestamp).total_seconds() > self.request_timeout:
                expired_requests.append(fingerprint)
        
        for fingerprint in expired_requests:
            del self.active_requests[fingerprint]
            app.logger.info(f"[CLEANUP] Removed expired request: {fingerprint}")
    
    def complete_request(self, fingerprint):
        """Mark request as completed"""
        if fingerprint in self.active_requests:
            del self.active_requests[fingerprint]
            app.logger.info(f"[COMPLETED] Request finished: {fingerprint}")
        else:
            app.logger.warning(f"[UNTRACKED] Completed unknown request: {fingerprint}")

request_deduplicator = RequestDeduplication()

# ---------- DATABASE CONFIG ----------
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres", 
    "user": "postgres",
    "password": "123456"
}

# ---------- LOCKING MECHANISM ----------
class ImportLockManager:
    def __init__(self, db_connection_config):
        self.db_config = db_connection_config
        self.lock_timeout = 300  # 5 minutes
    
    def acquire_lock(self, table_name, process_id):
        """Acquire lock for specific table"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Clean up expired locks
            cursor.execute("""
                DELETE FROM spigen.import_lock 
                WHERE locked_at < NOW() - INTERVAL '%s seconds'
            """, (self.lock_timeout,))
            
            # Try to acquire lock
            cursor.execute("""
                INSERT INTO spigen.import_lock (table_name, is_locked, locked_by, locked_at, process_id)
                VALUES (%s, TRUE, %s, NOW(), %s)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    is_locked = EXCLUDED.is_locked,
                    locked_by = EXCLUDED.locked_by,
                    locked_at = EXCLUDED.locked_at,
                    process_id = EXCLUDED.process_id
                WHERE spigen.import_lock.is_locked = FALSE
                RETURNING *
            """, (table_name, f"flask_app", process_id))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            return result is not None
            
        except Exception as e:
            print(f"Error acquiring lock: {e}")
            return False
    
    def release_lock(self, table_name, process_id):
        """Release the lock for specific table"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE spigen.import_lock 
                SET is_locked = FALSE, locked_by = NULL, process_id = NULL
                WHERE table_name = %s AND process_id = %s
            """, (table_name, process_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error releasing lock: {e}")
            return False

# Initialize lock manager
lock_manager = ImportLockManager(DB_CONFIG)

# ---------- TABLE COLUMN MAPPINGS ----------
TABLE_COLUMNS = {
    "stg_amz_b2b": [
        "seller_gstin", "invoice_number", "invoice_date", "transaction_type", "order_id",
        "shipment_id", "shipment_date", "order_date", "shipment_item_id", "quantity",
        "item_description", "asin", "hsn_sac", "sku", "product_tax_code", "bill_from_city",
        "bill_from_state", "bill_from_country", "bill_from_postal_code", "ship_from_city",
        "ship_from_state", "ship_from_country", "ship_from_postal_code", "ship_to_city",
        "ship_to_state", "ship_to_country", "ship_to_postal_code", "invoice_amount",
        "tax_exclusive_gross", "total_tax_amount", "cgst_rate", "sgst_rate", "utgst_rate",
        "igst_rate", "compensatory_cess_rate", "principal_amount", "principal_amount_basis",
        "cgst_tax", "sgst_tax", "utgst_tax", "igst_tax", "compensatory_cess_tax",
        "shipping_amount", "shipping_amount_basis", "shipping_cgst_tax", "shipping_sgst_tax",
        "shipping_utgst_tax", "shipping_igst_tax", "shipping_cess_tax", "gift_wrap_amount",
        "gift_wrap_amount_basis", "gift_wrap_cgst_tax", "gift_wrap_sgst_tax", "gift_wrap_utgst_tax",
        "gift_wrap_igst_tax", "gift_wrap_compensatory_cess_tax", "item_promo_discount",
        "item_promo_discount_basis", "item_promo_tax", "shipping_promo_discount",
        "shipping_promo_discount_basis", "shipping_promo_tax", "gift_wrap_promo_discount",
        "gift_wrap_promo_discount_basis", "gift_wrap_promo_tax", "tcs_cgst_rate", "tcs_cgst_amount",
        "tcs_sgst_rate", "tcs_sgst_amount", "tcs_utgst_rate", "tcs_utgst_amount", "tcs_igst_rate",
        "tcs_igst_amount", "warehouse_id", "fulfillment_channel", "payment_method_code",
        "bill_to_city", "bill_to_state", "bill_to_country", "bill_to_postalcode",
        "customer_bill_to_gstid", "customer_ship_to_gstid", "buyer_name", "credit_note_no",
        "credit_note_date", "irn_number", "irn_filing_status", "irn_date", "irn_error_code"
    ],
    "stg_amz_b2c": [
        "seller_gstin", "invoice_number", "invoice_date", "transaction_type", "order_id",
        "shipment_id", "shipment_date", "order_date", "shipment_item_id", "quantity",
        "item_description", "asin", "hsn_sac", "sku", "product_tax_code", "bill_from_city",
        "bill_from_state", "bill_from_country", "bill_from_postal_code", "ship_from_city",
        "ship_from_state", "ship_from_country", "ship_from_postal_code", "ship_to_city",
        "ship_to_state", "ship_to_country", "ship_to_postal_code", "invoice_amount",
        "tax_exclusive_gross", "total_tax_amount", "cgst_rate", "sgst_rate", "utgst_rate",
        "igst_rate", "compensatory_cess_rate", "principal_amount", "principal_amount_basis",
        "cgst_tax", "sgst_tax", "utgst_tax", "igst_tax", "compensatory_cess_tax",
        "shipping_amount", "shipping_amount_basis", "shipping_cgst_tax", "shipping_sgst_tax",
        "shipping_utgst_tax", "shipping_igst_tax", "shipping_cess_tax_amount", "gift_wrap_amount",
        "gift_wrap_amount_basis", "gift_wrap_cgst_tax", "gift_wrap_sgst_tax", "gift_wrap_utgst_tax",
        "gift_wrap_igst_tax", "gift_wrap_compensatory_cess_tax", "item_promo_discount",
        "item_promo_discount_basis", "item_promo_tax", "shipping_promo_discount",
        "shipping_promo_discount_basis", "shipping_promo_tax", "gift_wrap_promo_discount",
        "gift_wrap_promo_discount_basis", "gift_wrap_promo_tax", "tcs_cgst_rate", "tcs_cgst_amount",
        "tcs_sgst_rate", "tcs_sgst_amount", "tcs_utgst_rate", "tcs_utgst_amount", "tcs_igst_rate",
        "tcs_igst_amount", "warehouse_id", "fulfillment_channel", "payment_method_code",
        "credit_note_no", "credit_note_date"
    ],
    "stg_amz_payout": [
        "date_time", "settlement_id", "type", "order_id", "sku", "description", "quantity",
        "marketplace", "account_type", "fulfillment", "order_city", "order_state", "order_postal",
        "product_sales", "shipping_credits", "gift_wrap_credits", "promotional_rebates",
        "total_sales_tax_liable", "tcs_cgst", "tcs_sgst", "tcs_igst", "tds_section_194_o",
        "selling_fees", "fba_fees", "other_transaction_fees", "other", "total"
    ]
}

# ---------- PAYOUT COLUMN MAPPING ----------
PAYOUT_COLUMN_MAPPING = {
    "date_time": "date_time",
    "settlement_id": "settlement_id", 
    "type": "type",
    "order_id": "order_id",
    "sku": "sku",
    "description": "description",
    "quantity": "quantity",
    "marketplace": "marketplace",
    "account_type": "account_type",
    "fulfillment": "fulfillment",
    "order_city": "order_city",
    "order_state": "order_state", 
    "order_postal": "order_postal",
    "product_sales": "product_sales",
    "shipping_credits": "shipping_credits",
    "gift_wrap_credits": "gift_wrap_credits",
    "promotional_rebates": "promotional_rebates",
    # Handle both old and new column names for total sales tax liable
    "total_sales_tax_liable": "total_sales_tax_liable",
    "total_sales_tax_liable_gst_before_adjusting_tcs": "total_sales_tax_liable",
    "tcs_cgst": "tcs_cgst",
    "tcs_sgst": "tcs_sgst",
    "tcs_igst": "tcs_igst", 
    "tds_section_194_o": "tds_section_194_o",
    "selling_fees": "selling_fees",
    "fba_fees": "fba_fees",
    "other_transaction_fees": "other_transaction_fees",
    "other": "other",
    "total": "total"
}

# ---------- ENHANCED LOGGING ----------
def log_to_db(import_batch_id, procedure_name, step_name, status, records_affected=0, message="", error_details=""):
    """
    Enhanced logging function with better formatting
    """
    # Create more descriptive log messages
    status_icons = {
        'started': '[START]',
        'success': '[SUCCESS]', 
        'error': '[ERROR]',
        'warning': '[WARNING]'
    }
    
    icon = status_icons.get(status,  '[INFO]')
    log_message = f"{icon} Batch:{import_batch_id[:8]} | {procedure_name}:{step_name} | {status} | Records:{records_affected} | {message}"
    
    if status == 'error':
        app.logger.error(log_message)
        if error_details:
            app.logger.error(f" Error details: {error_details}")
    elif status == 'success':
        app.logger.info(log_message)
    else:
        app.logger.info(log_message)
    
    # Log to database
    log_conn = None
    try:
        log_conn = psycopg2.connect(**DB_CONFIG)
        with log_conn.cursor() as log_cur:
            log_cur.execute("""
                INSERT INTO spigen.import_logs 
                (import_batch_id, procedure_name, step_name, status, records_affected, message, error_details)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (import_batch_id, procedure_name, step_name, status, records_affected, message, error_details))
            log_conn.commit()
    except Exception as e:
        app.logger.error(f"Failed to write to log table: {e}")
    finally:
        if log_conn:
            log_conn.close()

# ---------- ENHANCED FILE VALIDATION ----------
def allowed_file(filename):
    """Check if the file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in [ext.replace('.', '') for ext in app.config['UPLOAD_EXTENSIONS']]

def secure_file_upload(file):
    """Secure file upload handling"""
    if file.filename == '':
        raise ValueError("No selected file")
    
    if not allowed_file(file.filename):
        raise ValueError(f"File type not allowed. Allowed types: {', '.join(app.config['UPLOAD_EXTENSIONS'])}")
    
    filename = secure_filename(file.filename)
    return filename

# ---------- OPTIMIZED COLUMN NORMALIZATION ----------
def normalize_column_names(df, table_name):
    """
    Optimized column normalization with caching and special payout handling
    """
    # Create cache key
    cache_key = f"{table_name}_{hash(tuple(df.columns))}"
    
    if hasattr(df, '_normalized') and getattr(df, '_cache_key', None) == cache_key:
        print(f"Columns already normalized for {table_name}, skipping...")
        return df
    
    print(f"Normalizing columns for {table_name}")
    print(f"Original columns: {list(df.columns)}")
    
    # Single-pass column cleaning
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r'[^a-z0-9]+', '_', regex=True)
        .str.strip('_')
    )
    
    print(f"After basic cleaning: {list(df.columns)}")
    
    # Special handling for payout files
    if table_name == 'stg_amz_payout':
        df = apply_payout_column_mapping(df)
    else:
        # Apply optimized fuzzy matching for other tables
        df = optimized_fuzzy_match(df, table_name)
    
    # Mark as processed
    df._normalized = True
    df._cache_key = cache_key
    
    print(f"Final columns for {table_name}: {list(df.columns)}")
    return df

def apply_payout_column_mapping(df):
    """
    Apply explicit column mapping for payout files to handle Amazon's column name changes
    """
    print("Applying payout column mapping...")
    
    # Create a mapping from current column names to target names
    mapping = {}
    for current_col in df.columns:
        # Check if this column matches any of our known payout column names
        if current_col in PAYOUT_COLUMN_MAPPING:
            mapping[current_col] = PAYOUT_COLUMN_MAPPING[current_col]
        else:
            # Keep the column as is if no mapping found
            mapping[current_col] = current_col
    
    # Apply the mapping
    df = df.rename(columns=mapping)
    
    # Ensure all expected payout columns exist
    for expected_col in TABLE_COLUMNS['stg_amz_payout']:
        if expected_col not in df.columns:
            df[expected_col] = None
            print(f"Added missing payout column: {expected_col}")
    
    return df

def optimized_fuzzy_match(df, table_name):
    """
    Optimized fuzzy matching with early termination
    """
    table_columns = TABLE_COLUMNS.get(table_name, [])
    csv_columns = list(df.columns)
    
    # Quick check for exact matches
    exact_matches = set(table_columns) & set(csv_columns)
    if len(exact_matches) == len(table_columns):
        return df
    
    mapping = {}
    used_columns = set(exact_matches)
    
    # Only process non-matching columns
    for table_col in table_columns:
        if table_col not in exact_matches:
            best_match = None
            
            for csv_col in csv_columns:
                if csv_col not in used_columns:
                    # Date column priority matching
                    if 'date' in table_col.lower() and any(var in csv_col.lower() for var in ['date', 'dt', 'timestamp']):
                        best_match = csv_col
                        break
                    
                    # Quick similarity check
                    clean_table = re.sub(r'[^a-z0-9]', '', table_col)
                    clean_csv = re.sub(r'[^a-z0-9]', '', csv_col)
                    
                    if clean_table == clean_csv or SequenceMatcher(None, clean_table, clean_csv).ratio() > 0.8:
                        best_match = csv_col
                        break
            
            if best_match:
                mapping[best_match] = table_col
                used_columns.add(best_match)
    
    if mapping:
        df = df.rename(columns=mapping)
    
    # Add missing columns
    missing_cols = set(table_columns) - set(df.columns)
    for col in missing_cols:
        df[col] = None
    
    return df

# ---------- OPTIMIZED DATE PROCESSING ----------
def optimized_date_parser(date_series):
    """
    Optimized date parser without deprecated parameters
    """
    if date_series.empty:
        return date_series
    
    date_str = date_series.astype(str).str.strip()
    
    # Try common formats in priority order
    formats = [
        '%Y-%m-%d %H:%M:%S',  # 2024-04-01 10:34:52
        '%Y-%m-%d',           # 2024-04-01
        '%d-%m-%Y',           # 01-04-2024
        '%d/%m/%Y',           # 01/04/2024
        '%d-%m-%y',           # 01-04-24
        '%d/%m/%y',           # 01/04/24
    ]
    
    for fmt in formats:
        parsed = pd.to_datetime(date_str, format=fmt, errors='coerce')
        if parsed.notna().sum() > len(parsed) * 0.7:  # 70% success threshold
            return parsed
    
    # Final attempt with mixed formats
    return pd.to_datetime(date_str, errors='coerce')

# ---------- ENHANCED NUMERIC PROCESSING ----------
def enhanced_batch_process_numeric_columns(df):
    """
    Enhanced numeric column processing that handles comma thousands separators
    """
    numeric_patterns = ['amount', 'price', 'tax', 'quantity', 'rate', 'total', 'sales', 'credits', 'fees']
    numeric_cols = [col for col in df.columns if any(pattern in col for pattern in numeric_patterns)]
    
    if not numeric_cols:
        return df
    
    print(f"Batch processing {len(numeric_cols)} numeric columns")
    
    # Convert to string and clean in batch with enhanced comma handling
    for col in numeric_cols:
        print(f"   Cleaning column: {col}")
        original_sample = df[col].head(3).tolist()
        
        # Enhanced cleaning: remove commas, currency symbols, and other non-numeric characters
        df[col] = (
            df[col].astype(str)
            .str.replace(',', '', regex=False)  # Remove commas first - CRITICAL FIX
            .str.replace(r'[^\d\.\-]', '', regex=True)  # Keep only digits, decimal, and minus
            .replace(['', 'nan', 'none', 'null', 'NaN', 'NULL', 'None', 'N/A', 'n/a', '--', 'NULL'], '0', regex=False)
        )
        
        # Convert to numeric
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Log cleaning results for debugging
        cleaned_sample = df[col].head(3).tolist()
        print(f"     Original: {original_sample} -> Cleaned: {cleaned_sample}")
    
    return df

# ---------- OPTIMIZED FILE PROCESSORS ----------
def process_b2b_b2c_file(filepath):
    """Optimized processor for B2B/B2C files with enhanced error handling"""
    print(f"\nProcessing B2B/B2C file: {filepath}")
    
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            if filepath.endswith('.csv'):
                # Use pandas with thousands parameter to handle comma separators during read
                df = pd.read_csv(filepath, encoding=encoding, thousands=',', keep_default_na=True, na_values=['', 'NULL', 'null', 'N/A', 'n/a'])
            else:
                df = pd.read_excel(filepath, keep_default_na=True, na_values=['', 'NULL', 'null', 'N/A', 'n/a'])
            print(f"Successfully read with encoding: {encoding}")
            return df
        except (UnicodeDecodeError, Exception) as e:
            print(f"Failed with encoding {encoding}: {e}")
            continue
    
    # Fallback
    try:
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='utf-8', errors='replace', thousands=',')
        else:
            df = pd.read_excel(filepath)
        print("Used UTF-8 with error replacement")
        return df
    except Exception as e:
        raise Exception(f"Could not read file: {str(e)}")

def smart_payout_processor(filepath):
    """
    Robust processor for Amazon payout files.
    Handles:
    - Multiple encodings
    - Multi-column CSVs or single-column CSVs
    - Headers with descriptions
    - Automatic delimiter detection
    - Column normalization
    - Numeric/date parsing
    """
    import io, re, pandas as pd

    print(f"\nProcessing payout file: {filepath}")

    # Step 1: Read raw file text with multiple encodings
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    raw_text = None
    used_encoding = None
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc, errors='replace') as f:
                raw_text = f.read()
            used_encoding = enc
            print(f"Successfully read file with encoding: {enc}")
            break
        except Exception:
            continue
    if raw_text is None:
        raise Exception("Could not read file in any encoding")

    # Step 2: Detect header line (line containing required payout fields)
    header_index = None
    for i, line in enumerate(raw_text.splitlines()):
        line_lower = line.lower()
        if all(k in line_lower for k in ['date/time','settlement id','type','order id']):
            header_index = i
            print(f"Header found at line {i+1}")
            break
    if header_index is None:
        raise Exception("Could not find header line in file")

    # Step 3: Auto-detect delimiter from header line
    first_line = raw_text.splitlines()[header_index]
    possible_delims = [',','\t',';','|']
    delim_counts = {d:first_line.count(d) for d in possible_delims}
    best_delim = max(delim_counts, key=delim_counts.get)
    print(f"Detected delimiter: '{best_delim}' (counts: {delim_counts})")

    # Step 4: Try reading normally
    df = pd.read_csv(
        io.StringIO(raw_text),
        skiprows=header_index,
        delimiter=best_delim,
        engine='python',
        on_bad_lines='skip'
    )

    # Step 5: Handle single-column case
    if df.shape[1] == 1:
        print("Single-column file detected. Splitting manually...")
        # Split each row by detected delimiter
        df = df[df.columns[0]].str.split(best_delim, expand=True)

    # Step 6: Normalize columns
    df = normalize_column_names(df, 'stg_amz_payout')

    # Step 7: Clean numeric columns
    numeric_cols = [
        'product_sales','shipping_credits','gift_wrap_credits','promotional_rebates',
        'total_sales_tax_liable','tcs_cgst','tcs_sgst','tcs_igst','tds_section_194_o',
        'selling_fees','fba_fees','other_transaction_fees','other','total'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(',', '', regex=False)
                .str.replace(r'[^\d\.-]', '', regex=True)
                .replace(['', 'nan', 'none', 'null'], '0', regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Step 8: Parse date column
    if 'date_time' in df.columns:
        df['date_time'] = optimized_date_parser(df['date_time'])

    print(f"Columns loaded: {len(df.columns)}, Rows: {len(df)}")
    print(df.head(5))
    return df

# ---------- BATCH PROCESSING FUNCTIONS ----------
def batch_process_date_columns(df, table_name):
    """Process date columns in batch"""
    date_columns = ['invoice_date', 'shipment_date', 'order_date', 'credit_note_date', 'irn_date']
    existing_dates = [col for col in date_columns if col in df.columns]
    
    if not existing_dates:
        return df
    
    print(f"Batch processing {len(existing_dates)} date columns")
    
    for date_col in existing_dates:
        if df[date_col].notna().any():
            null_before = df[date_col].isnull().sum()
            df[date_col] = optimized_date_parser(df[date_col])
            null_after = df[date_col].isnull().sum()
            
            if null_after > null_before:
                print(f"   {date_col}: {null_after - null_before} new nulls after parsing")
    
    return df

# ---------- OPTIMIZED DATAFRAME OPTIMIZATION ----------
def optimize_dataframe(df, table_name):
    """Optimized DataFrame processing with reduced redundancy"""
    print(f"Optimizing for table: {table_name}")
    
    # Check if already processed
    if hasattr(df, '_optimized') and getattr(df, '_optimized_for') == table_name:
        print("DataFrame already optimized, skipping...")
        return df
    
    original_shape = df.shape
    
    # Single normalization pass
    df = normalize_column_names(df, table_name)
    
    # Batch process dates and use enhanced numeric processing
    df = batch_process_date_columns(df, table_name)
    df = enhanced_batch_process_numeric_columns(df)  # Use enhanced version
    
    # Clean string columns
    str_cols = df.select_dtypes(include=['object']).columns
    for col in str_cols:
        empty_count = (df[col] == '').sum()
        if empty_count > 0:
            df.loc[df[col] == '', col] = None
    
    # Mark as optimized
    df._optimized = True
    df._optimized_for = table_name
    
    print(f"Optimized: {original_shape} → {df.shape}")
    validate_dataframe(df, table_name)
    
    return df

# ---------- OPTIMIZED VALIDATION ----------
def validate_dataframe(df, table_name):
    """Optimized data validation"""
    print(f"\nVALIDATION REPORT for {table_name}:")
    print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # Null value summary
    null_summary = []
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            null_summary.append(f"   {col}: {null_count} nulls ({null_count/len(df)*100:.1f}%)")
    
    if null_summary:
        print("NULL VALUES:")
        print('\n'.join(null_summary[:10]))  # Show first 10 only
    
    # Date column status
    date_cols = ['invoice_date', 'shipment_date', 'order_date', 'credit_note_date', 'irn_date']
    existing_dates = [col for col in date_cols if col in df.columns]
    
    if existing_dates:
        print("\nDATE STATUS:")
        for col in existing_dates:
            null_count = df[col].isnull().sum()
            unique_dates = df[col].nunique()
            print(f"   {col}: {null_count} nulls, {unique_dates} unique dates")

    # Numeric column validation
    numeric_patterns = ['amount', 'price', 'tax', 'quantity', 'rate', 'total', 'sales', 'credits', 'fees']
    numeric_cols = [col for col in df.columns if any(pattern in col for pattern in numeric_patterns)]
    
    if numeric_cols:
        print("\nNUMERIC COLUMNS STATUS:")
        for col in numeric_cols[:5]:  # Show first 5 numeric columns
            non_numeric = df[col].apply(lambda x: not isinstance(x, (int, float))).sum()
            if non_numeric > 0:
                print(f"   {col}: {non_numeric} non-numeric values")

    return df

def check_procedure_signature():
    """Check the signature of the reconciliation procedure"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Check procedure parameters
                cur.execute("""
                    SELECT 
                        p.proname as procedure_name,
                        p.pronargs as num_parameters,
                        format_type(t.oid, NULL) as data_type
                    FROM pg_proc p
                    LEFT JOIN pg_type t ON p.prorettype = t.oid
                    WHERE p.proname = 'process_reconciliation_opt'
                    AND p.pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'spigen');
                """)
                proc_info = cur.fetchone()
                
                if proc_info:
                    app.logger.info(f"Procedure info: {proc_info}")
                
                # Check parameters in detail
                cur.execute("""
                    SELECT 
                        p.proname,
                        unnest(p.proargnames) as param_name,
                        format_type(unnest(p.proargtypes), NULL) as param_type
                    FROM pg_proc p
                    WHERE p.proname = 'process_reconciliation_opt'
                    AND p.pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'spigen');
                """)
                params = cur.fetchall()
                app.logger.info(f"Procedure parameters: {params}")
                
                return proc_info, params
                
    except Exception as e:
        app.logger.error(f"Error checking procedure signature: {e}")
        return None, None

# Call this function somewhere to debug
# check_procedure_signature()

# ---------- EMERGENCY ROUTES FOR STUCK IMPORTS ----------
@app.route('/api/cancel-import/<import_id>', methods=['POST', 'GET'])
def cancel_import(import_id):
    """Emergency route to cancel stuck imports"""
    if import_id in progress_tracker.active_imports:
        del progress_tracker.active_imports[import_id]
        app.logger.info(f"Force cancelled import: {import_id}")
        flash(f"Import {import_id} has been cancelled", "warning")
        return jsonify({'status': 'cancelled', 'message': f'Import {import_id} cancelled'})
    else:
        return jsonify({'status': 'error', 'message': 'Import not found'}), 404

@app.route('/debug-import/<import_id>')
def debug_import(import_id):
    """Debug stuck imports"""
    import_info = progress_tracker.active_imports.get(import_id, {})
    return jsonify({
        'import_id': import_id,
        'status': import_info.get('status', 'not_found'),
        'current_step': import_info.get('current_step', 'unknown'),
        'progress': import_info.get('progress', 0),
        'last_update': import_info.get('last_update', 'never')
    })

# [Keep all your existing utility functions the same - they're working well]
# optimized_date_parser, enhanced_batch_process_numeric_columns, process_b2b_b2c_file, 
# smart_payout_processor, normalize_column_names, optimize_dataframe, etc.

# [Keep all your existing routes the same - they're working well]
# upload, import_progress, api_import_status, logs, etc.


# ---------- ENHANCED ERROR HANDLING ----------
@app.errorhandler(413)
def too_large(e):
    """Handle file too large errors"""
    flash("File too large. Maximum size is 200MB.", "danger")
    return redirect(url_for('upload'))

@app.errorhandler(500)
def internal_error(e):
    """Handle internal server errors"""
    flash("An internal error occurred. Please try again.", "danger")
    return redirect(url_for('upload'))

# ---------- BASIC ROUTES ----------
@app.route('/')
def home():
    """Home page route"""
    if 'user' in session:
        return redirect(url_for('upload'))
    return render_template('home.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page"""
    if 'user' in session:
        return redirect(url_for('upload'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if username and password:
            session['user'] = username
            flash('Login successful!', 'success')
            return redirect(url_for('upload'))
        else:
            flash('Please enter both username and password', 'danger')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Logout user"""
    session.pop('user', None)
    flash('You have been logged out', 'info')
    return redirect(url_for('home'))

@app.route('/debug-session')
def debug_session():
    """Debug route to check session state"""
    return jsonify({
        'user_in_session': 'user' in session,
        'session_keys': list(session.keys()),
        'user_agent': request.headers.get('User-Agent')
    })

@app.route('/test')
def test_route():
    return "Test route works! Session user: " + str(session.get('user', 'Not logged in'))

# ---------- MAIN APPLICATION ROUTES ----------
@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if 'user' not in session:
        flash("Please login first", "warning")
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        try:
            # Generate request fingerprint for deduplication
            fingerprint = request_deduplicator.generate_request_fingerprint(request)
            
            # Check for duplicate requests
            if request_deduplicator.is_duplicate_request(fingerprint):
                flash("This upload request is already being processed. Please wait for it to complete.", "warning")
                return redirect(url_for('upload'))
            
            platform = request.form.get('platform')
            if not platform:
                flash("Please select a platform", "danger")
                return redirect(url_for('upload'))
            
            # Process files and get file paths
            files_to_process = []
            import_batch_id = str(uuid.uuid4())[:16]
            
            # Process B2B file
            b2b_file = request.files.get('b2b_file')
            if b2b_file and b2b_file.filename:
                b2b_filename = secure_file_upload(b2b_file)
                b2b_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{import_batch_id}_b2b_{b2b_filename}")
                b2b_file.save(b2b_path)
                files_to_process.append({
                    'type': 'b2b',
                    'file_path': b2b_path,
                    'filename': b2b_filename
                })
            
            # Process B2C file
            b2c_file = request.files.get('b2c_file')
            if b2c_file and b2c_file.filename:
                b2c_filename = secure_file_upload(b2c_file)
                b2c_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{import_batch_id}_b2c_{b2c_filename}")
                b2c_file.save(b2c_path)
                files_to_process.append({
                    'type': 'b2c',
                    'file_path': b2c_path,
                    'filename': b2c_filename
                })
            
            # Process Payout file
            payout_file = request.files.get('payout_file')
            if payout_file and payout_file.filename:
                payout_filename = secure_file_upload(payout_file)
                payout_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{import_batch_id}_payout_{payout_filename}")
                payout_file.save(payout_path)
                files_to_process.append({
                    'type': 'payout',
                    'file_path': payout_path,
                    'filename': payout_filename
                })
            
            if not files_to_process:
                flash("Please select at least one file to upload", "danger")
                return redirect(url_for('upload'))
            
            # Start the import process FIRST
            progress_tracker.start_import_process(import_batch_id, files_to_process, platform)
            
            # Mark request as completed in deduplication
            request_deduplicator.complete_request(fingerprint)
            
            # Redirect to progress page WITH the import_batch_id
            flash(f"Import started successfully! Tracking ID: {import_batch_id}", "success")
            return redirect(url_for('import_progress', import_id=import_batch_id))
            
        except Exception as e:
            flash(f'Error during upload: {str(e)}', 'danger')
            return redirect(url_for('upload'))
    
    return render_template('upload.html')

@app.route('/import_progress')
def import_progress():
    """Display the import progress page"""
    if 'user' not in session:
        flash("Please login first", "warning")
        return redirect(url_for('login'))
    
    # Get the import_id from URL parameters
    import_id = request.args.get('import_id')

        # If no import_id provided, get the most recent active import
    if not import_id and progress_tracker.active_imports:
        import_id = list(progress_tracker.active_imports.keys())[-1]  # Get most recent
    
    # Get active imports for display
    active_imports = list(progress_tracker.active_imports.keys())
    
    # Pass current time for initial log timestamp
    from datetime import datetime
    now = datetime.now()
    
    return render_template('import_progress.html', 
                         import_id=import_id,
                         active_imports=active_imports,
                         now=now)

@app.route('/logs')
def view_logs():
    if 'user' not in session:
        flash("Please login first", "warning")
        return redirect(url_for('login'))
    
    # Read from log file
    log_entries = []
    try:
        with open('flask_import.log', 'r') as log_file:
            log_entries = log_file.readlines()[-100:]  # Last 100 entries
    except FileNotFoundError:
        log_entries = ["No log file found yet."]
    
    # Get database logs
    db_logs = []
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT import_batch_id, procedure_name, step_name, status, 
                           records_affected, message, created_at 
                    FROM spigen.import_logs 
                    ORDER BY created_at DESC 
                    LIMIT 50
                """)
                db_logs = cur.fetchall()
    except Exception as e:
        db_logs = [("Error", "Error", "Error", "error", 0, f"Failed to load DB logs: {str(e)}", datetime.now())]
    
    # Get active requests for monitoring
    active_requests = list(request_deduplicator.active_requests.keys())
    
    # Get active imports from progress tracker
    active_imports = list(progress_tracker.active_imports.keys())
    
    return render_template('logs.html', file_logs=log_entries, db_logs=db_logs, 
                         active_requests=active_requests, active_imports=active_imports)

@app.route('/api/recent-logs')
def api_recent_logs():
    """API endpoint to get recent logs for the upload page"""
    try:
        # Get recent logs from database
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT created_at, message, status 
                    FROM spigen.import_logs 
                    ORDER BY created_at DESC 
                    LIMIT 20
                """)
                db_logs = cur.fetchall()
        
        # Format logs for frontend
        logs = []
        for log in db_logs:
            logs.append({
                'timestamp': log[0].isoformat(),
                'message': log[1],
                'level': log[2]  # status field becomes level
            })
        
        return jsonify(logs)
        
    except Exception as e:
        app.logger.error(f"Error fetching recent logs: {e}")
        return jsonify([])

@app.route('/admin/requests')
def view_active_requests():
    """Admin endpoint to view active requests"""
    if 'user' not in session:
        flash("Please login first", "warning")
        return redirect(url_for('login'))
    
    active_requests = [
        {
            'fingerprint': fp,
            'started': started.strftime('%Y-%m-%d %H:%M:%S'),
            'age_seconds': (datetime.now() - started).total_seconds()
        }
        for fp, started in request_deduplicator.active_requests.items()
    ]
    
    return render_template('active_requests.html', active_requests=active_requests)


@app.route('/cancel-all-imports', methods=['POST', 'GET'])
def cancel_all_imports():
    """Cancel all active imports"""
    active_count = len(progress_tracker.active_imports)
    progress_tracker.active_imports.clear()
    app.logger.info(f"🚨 Cancelled all {active_count} active imports")
    flash(f"Cancelled {active_count} active imports", "warning")
    return redirect(url_for('upload'))



@app.route('/check-db')
def check_db():
    """Check database connection status"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 as test")
            result = cur.fetchone()
        conn.close()
        return f"Database OK: {result}"
    except Exception as e:
        return f"Database ERROR: {str(e)}"
    
@app.route('/api/import-status')
def api_import_status():
    """API endpoint to get current import status"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    import_id = request.args.get('import_id')
    
    # If specific import ID is provided, return only that
    if import_id:
        import_data = progress_tracker.get_progress(import_id)
        if not import_data:
            return jsonify({'error': 'Import not found'}), 404
        
        # Ensure we have the required fields
        response_data = {
            'import_id': import_id,
            'status': import_data.get('status', 'unknown'),
            'current_step': import_data.get('current_step', 'Unknown step'),
            'progress': import_data.get('progress', 0),
            'start_time': import_data['start_time'].isoformat(),
            'last_update': import_data.get('last_update', import_data['start_time']).isoformat(),
            'logs': import_data.get('logs', [])[-20:],
            'file_types': list(import_data.get('file_types', [])),
            'uploaded_months': list(import_data.get('uploaded_months', [])),
            'files': [f.get('filename', 'Unknown file') for f in import_data.get('files', [])]
        }
        
        return jsonify(response_data)
    
    # Otherwise return all active imports
    active_imports = {}
    for import_id, import_data in progress_tracker.active_imports.items():
        active_imports[import_id] = {
            'status': import_data.get('status', 'unknown'),
            'current_step': import_data.get('current_step', 'Unknown step'),
            'progress': import_data.get('progress', 0),
            'start_time': import_data['start_time'].isoformat(),
            'last_update': import_data.get('last_update', import_data['start_time']).isoformat(),
            'logs': import_data.get('logs', [])[-10:],
            'file_types': list(import_data.get('file_types', [])),
            'uploaded_months': list(import_data.get('uploaded_months', [])),
            'files': [f.get('filename', 'Unknown file') for f in import_data.get('files', [])]
        }
    
    return jsonify({
        'active_imports': active_imports,
        'total_active': len(active_imports)
    })

@app.route('/test-progress')
def test_progress():
    """Test route to verify progress page works"""
    return render_template('import_progress.html', 
                         import_id='test-123', 
                         active_imports=['test-123'],
                         now=datetime.now())

# Fix the cleanup function
import atexit
@atexit.register
def cleanup_on_shutdown():
    """Clean up all active requests on application shutdown"""
    app.logger.info("Application shutting down - cleaning up active requests")
    request_deduplicator.active_requests.clear()
    progress_tracker.executor.shutdown(wait=False)
    app.logger.info("All active requests cleared")

def create_app():
    """Application factory pattern for better Flask compatibility"""
    # Create uploads directory if it doesn't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Clean up any expired requests on startup
    request_deduplicator.cleanup_expired_requests()
    
    # Register graceful shutdown handlers
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    
    # Start background cleanup thread
    def periodic_cleanup():
        while True:
            time.sleep(300)  # Run every 5 minutes
            progress_tracker.cleanup_stuck_imports()
            request_deduplicator.cleanup_expired_requests()
    
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    
    return app

# For python app.py
if __name__ == '__main__':
    app = create_app()
    app.logger.info("Flask application started with SMART reconciliation system (python app.py)")
    
    try:
        app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        graceful_shutdown()
    except Exception as e:
        app.logger.error(f"Application error: {e}")
        graceful_shutdown()

# For flask run
app = create_app()