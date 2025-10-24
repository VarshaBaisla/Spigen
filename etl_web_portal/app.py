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
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 200MB max file size
app.config['UPLOAD_EXTENSIONS'] = ['.csv', '.xlsx', '.xls']
app.config['UPLOAD_FOLDER'] = 'uploads'
# Add these additional configurations
app.config['MAX_REQUEST_SIZE'] = 500 * 1024 * 1024  # 500MB
app.config['REQUEST_TIMEOUT'] = 3600  # 1 hour timeout for large uploads

#-----------------------------  LOGIN DETAILS-------------------

VALID_USERS = {
    "admin": "admin123", #username:password
    "atyab@orbis.com": "Admin@123456",
    "user": "password123"
}

# ---------- DATABASE CONFIG ----------
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres", 
    "user": "postgres",
    "password": "123456",
    "connect_timeout": 10,
    "options": "-c statement_timeout=3600000 -c lock_timeout=3600000"
}
# Add this after your DB_CONFIG
def get_database_columns_fast(table_name, engine):
    """ULTRA-FAST database column lookup with proper schema handling"""
    try:
        # Extract table name without schema prefix
        if '.' in table_name:
            schema, table = table_name.split('.')
        else:
            schema = 'spigen'  # Your schema name
            table = table_name.replace('stg_amz_', '')  # Remove prefix if present
        
        with engine.cursor() as cur:
            # FAST query with exact schema/table match
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            
            columns = [row[0] for row in cur.fetchall()]
            return columns
    except Exception as e:
        app.logger.error(f"Fast column lookup failed for {table_name}: {e}")
        return []
    
def get_db_connection():
    """Enhanced database connection with timeout settings"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 3600000;")
            cur.execute("SET lock_timeout = 3600000;")
        return conn
    except Exception as e:
        app.logger.error(f"Database connection failed: {e}")
        raise

# ---------- REQUEST DEDUPLICATION (FIXED) ----------
class RequestDeduplication:
    def __init__(self):
        self.active_requests = {}
        self.request_timeout = 600  # 10 minutes
    
    def generate_request_fingerprint(self, request, session_data):
        """Generate unique fingerprint for each upload request - FIXED: accepts session_data"""
        fingerprint_data = {
            'user': session_data.get('user', 'anonymous'),  # Use passed session_data
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

# ---------- PROGRESS TRACKER (FIXED) ----------
class ProgressTracker:
    def __init__(self):
        self.active_imports = {}
        self.executor = ThreadPoolExecutor(max_workers=3)
        self.cancellation_events = {}
    
    def start_import_process(self, import_batch_id, files_to_process, platform):
        """Start the automated import process"""
        # Create cancellation event for this import
        self.cancellation_events[import_batch_id] = threading.Event()

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
            'file_types': set(),
            'files_processed': 0,
            'total_files': len(files_to_process),
            'cancelled': False,
            'last_update': datetime.now()  # CRITICAL: Add this field
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

    # In your ProgressTracker class, update these methods:

    def request_cancellation(self, import_batch_id):
        """Request cancellation of an import - ENHANCED VERSION"""
        if import_batch_id not in self.active_imports:
            return False
        
        import_data = self.active_imports[import_batch_id]
        
        # Only allow cancellation if import is still processing
        if import_data['status'] not in ['processing', 'starting']:
            return False
        
        # Mark for cancellation and set the cancellation event
        import_data['cancelled'] = True
        import_data['status'] = 'cancelled'
        import_data['current_step'] = 'Cancellation in progress...'
        import_data['progress'] = 0
        import_data['last_update'] = datetime.now()
        
        # Trigger cancellation event to stop background threads
        if import_batch_id in self.cancellation_events:
            self.cancellation_events[import_batch_id].set()
        
        self._add_log(import_batch_id, 'Import cancellation requested by user', 'warning')
        app.logger.info(f"[{import_batch_id}] Import cancellation requested - cancellation event set")
        return True

    def is_cancelled(self, import_batch_id):
        """Check if cancellation was requested for this import"""
        if import_batch_id not in self.cancellation_events:
            return False
        return self.cancellation_events[import_batch_id].is_set() or self.active_imports.get(import_batch_id, {}).get('cancelled', False)

    def _run_automated_import(self, import_batch_id, files_to_process, platform):
        """Run the complete automated import process - WITH PROPER CANCELLATION"""
        try:
            # Update status to processing immediately
            self._update_progress(import_batch_id, 'Starting file processing...', 5, 'processing')
            
            # Check for cancellation at the start
            if self.is_cancelled(import_batch_id):
                self._handle_cancellation(import_batch_id)
                return
            
            # STEP 1: File upload and staging
            self._update_progress(import_batch_id, 'Uploading files to staging tables...', 10)

            # Check for cancellation before each major step
            if self.is_cancelled(import_batch_id):
                self._handle_cancellation(import_batch_id)
                return
            
            staging_results = self._process_files_to_staging(import_batch_id, files_to_process, platform)
            
            # Check if staging was cancelled
            if staging_results.get('cancelled'):
                self._handle_cancellation(import_batch_id)
                return
                
            if not staging_results['success']:
                self._update_progress(import_batch_id, f'Staging failed: {staging_results["error"]}', 100, 'error')
                return

            # STEP 2: Data processing to final tables
            self._update_progress(import_batch_id, 'Processing data in final tables...', 40)
            
            # Check for cancellation
            if self.is_cancelled(import_batch_id):
                self._handle_cancellation(import_batch_id)
                return
            
            processing_results = self._process_final_tables(import_batch_id, staging_results['file_types'], platform)
            
            # Check if processing was cancelled
            if processing_results.get('cancelled'):
                self._handle_cancellation(import_batch_id)
                return
                
            if not processing_results['success']:
                self._update_progress(import_batch_id, f'Processing failed: {processing_results["error"]}', 100, 'error')
                return

            # STEP 3: Smart Reconciliation (only if not cancelled)
            if not self.is_cancelled(import_batch_id):
                uploaded_months = self.active_imports[import_batch_id].get('uploaded_months', [])
                file_types = self.active_imports[import_batch_id].get('file_types', set())
                
                if uploaded_months:
                    self._update_progress(import_batch_id, 'Checking if reconciliation is needed...', 65)
                    
                    # Check for cancellation
                    if self.is_cancelled(import_batch_id):
                        self._handle_cancellation(import_batch_id)
                        return
                        
                    self._add_log(import_batch_id, f'Uploaded files for months: {uploaded_months}', 'info')
                    
                    # Check for reconciliation
                    should_reconcile, reconcile_message = self._should_run_reconciliation(uploaded_months, file_types)
                    
                    if should_reconcile:
                        self._add_log(import_batch_id, f'Running reconciliation for: {reconcile_message}', 'info')
                        
                        for month_year in reconcile_message:
                            self._update_progress(import_batch_id, f'Running reconciliation for {month_year}...', 70)
                            
                            # Check for cancellation before reconciliation
                            if self.is_cancelled(import_batch_id):
                                self._handle_cancellation(import_batch_id)
                                return
                                
                            recon_results = self._run_reconciliation_for_month(month_year, import_batch_id)
                            
                            if not recon_results['success']:
                                self._add_log(import_batch_id, f'Reconciliation failed for {month_year}: {recon_results["error"]}', 'warning')
                            else:
                                self._add_log(import_batch_id, f'Reconciliation completed for {month_year}', 'success')
                    else:
                        self._add_log(import_batch_id, f'Skipping reconciliation: {reconcile_message}', 'info')

            # Check for cancellation before final completion
            if self.is_cancelled(import_batch_id):
                self._handle_cancellation(import_batch_id)
                return
            
            # STEP 4: Completion
            self._update_progress(import_batch_id, 'Import completed successfully!', 100, 'success')
            self._add_log(import_batch_id, 'All import procedures completed successfully', 'success')
            
        except Exception as e:
            error_msg = f'Import process failed: {str(e)}'
            self._update_progress(import_batch_id, error_msg, 100, 'error')
            self._add_log(import_batch_id, error_msg, 'error')
            app.logger.error(f"[{import_batch_id}] {error_msg}")

    def _handle_cancellation(self, import_batch_id):
        """Handle import cancellation"""
        self._update_progress(import_batch_id, 'Import cancelled by user', 0, 'cancelled')
        self._add_log(import_batch_id, 'Import process was cancelled by user', 'warning')
        app.logger.info(f"[{import_batch_id}] Import process successfully cancelled")
        
        # Clear file tracking for cancelled files
        import_files = self.active_imports.get(import_batch_id, {}).get('files', [])
        for file_info in import_files:
            filename = file_info.get('filename')
            file_type = file_info.get('type')
            if filename and file_type:
                clear_single_file_tracking(filename, file_type)
                app.logger.info(f"[{import_batch_id}] Cleared import tracking for cancelled file: {filename}")

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
    
    def should_run_reconciliation_optimized(self, import_batch_id):
        """Check if reconciliation should run - OPTIMIZED VERSION"""
        try:
            current_import = self.active_imports.get(import_batch_id, {})
            uploaded_files = [f['type'] for f in current_import.get('files', [])]
            uploaded_months = current_import.get('uploaded_months', [])
            
            # Only run reconciliation if we have BOTH sales and payout data
            has_sales = any(ft in uploaded_files for ft in ['b2b', 'b2c'])
            has_payout = 'payout' in uploaded_files
            
            if not (has_sales and has_payout):
                app.logger.info(f"[{import_batch_id}] Skipping reconciliation - missing files. Sales: {has_sales}, Payout: {has_payout}")
                return False, "Missing sales or payout data"
            
            # Check if we have complete data for any month
            reconcilable_months = self._find_reconcilable_months_for_uploaded(uploaded_months)
            
            if not reconcilable_months:
                app.logger.info(f"[{import_batch_id}] Skipping reconciliation - no complete months found")
                return False, "No months with complete B2B+B2C+Payout data"
            
            # Limit to most recent month to avoid processing too much history
            target_month = max(reconcilable_months) if reconcilable_months else None
            
            if target_month:
                app.logger.info(f"[{import_batch_id}] Reconciliation approved for month: {target_month}")
                return True, [target_month]
            else:
                return False, "No suitable month found"
                
        except Exception as e:
            app.logger.error(f"[{import_batch_id}] Error in optimized reconciliation check: {e}")
            return False, f"Error: {str(e)}"

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
        """Extract month from filename with pattern matching - IMPROVED"""
        filename_upper = filename.upper()
        
        month_map = {
            'JANUARY': '01', 'FEBRUARY': '02', 'MARCH': '03', 'APRIL': '04',
            'MAY': '05', 'JUNE': '06', 'JULY': '07', 'AUGUST': '08',
            'SEPTEMBER': '09', 'OCTOBER': '10', 'NOVEMBER': '11', 'DECEMBER': '12'
        }
        
        month_abbr_map = {
            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
            'JUL': '07', 'AUG': '08', 'SEP': '09', 'SEPT': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
        }
        
        # Pattern 1: MTR_B2B-APRIL-2024.csv or MTR_B2C-MAY-2024.csv
        b2b_pattern = r'MTR_B2B-([A-Z]+)-(\d{4})'
        b2c_pattern = r'MTR_B2C-([A-Z]+)-(\d{4})'
        
        for pattern in [b2b_pattern, b2c_pattern]:
            match = re.search(pattern, filename_upper)
            if match:
                month_name, year = match.groups()
                if month_name in month_map:
                    return f"{year}-{month_map[month_name]}"
                elif month_name in month_abbr_map:
                    return f"{year}-{month_abbr_map[month_name]}"
        
        # Pattern 2: Payout files - AMZ_Payout_Apr_2024.csv
        payout_patterns = [
            r'AMZ[_-]Payout[_-]([A-Z]+)[_-](\d{4})',
            r'PAYOUT[_-]([A-Z]+)[_-](\d{4})',
            r'(\d{4})[_-]([A-Z]+)[_-]PAYOUT'
        ]
        
        for pattern in payout_patterns:
            match = re.search(pattern, filename_upper)
            if match:
                if len(match.groups()) == 2:
                    month_name, year = match.groups()
                else:
                    year, month_name = match.groups()
                    
                if month_name in month_map:
                    return f"{year}-{month_map[month_name]}"
                elif month_name in month_abbr_map:
                    return f"{year}-{month_abbr_map[month_name]}"
        
        # Pattern 3: Simple month-year patterns
        simple_pattern = r'(\d{4})[_-](\d{2})'
        match = re.search(simple_pattern, filename_upper)
        if match:
            year, month = match.groups()
            return f"{year}-{month}"
        
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
        """Run reconciliation for a specific month-year with timeout protection"""
        try:
            app.logger.info(f"Running reconciliation for month: {month_year}")
            start_time = datetime.now()
            
            # Update progress with timeout information
            self._update_progress(import_batch_id, 
                                f'Starting reconciliation for {month_year}...', 
                                85)
            self._add_log(import_batch_id, 
                        f'Reconciliation started for {month_year}. This may take 10-30 minutes for large datasets.', 
                        'info')
            
            conn = get_db_connection()
            with conn.cursor() as cur:
                # SET OPTIMIZED TIMEOUTS
                cur.execute("SET statement_timeout = 1800000;")  # 30 minutes
                cur.execute("SET lock_timeout = 1800000;")  
                cur.execute("SET idle_in_transaction_session_timeout = 1800000;")
                
                # Update progress before calling the procedure
                self._update_progress(import_batch_id, 
                                    f'Running comprehensive reconciliation (processing historical data)...', 
                                    87)
                
                # Add intermediate progress updates during long-running operation
                def progress_monitor():
                    time_elapsed = 0
                    while time_elapsed < 1800:  # 30 minutes max
                        time.sleep(30)  # Check every 30 seconds
                        time_elapsed += 30
                        
                        # Update progress to show it's still running
                        if time_elapsed % 60 == 0:  # Every minute
                            minutes = time_elapsed // 60
                            self._add_log(import_batch_id, 
                                        f'Reconciliation in progress... {minutes} minutes elapsed', 
                                        'info')
                        
                        # Check for cancellation
                        if self.is_cancelled(import_batch_id):
                            break
                
                # Start progress monitor in separate thread
                monitor_thread = threading.Thread(target=progress_monitor, daemon=True)
                monitor_thread.start()
                
                # Call the reconciliation procedure
                cur.execute("CALL spigen.process_reconciliation_opt()")
                conn.commit()
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            minutes = int(processing_time // 60)
            seconds = int(processing_time % 60)
            
            # Update progress after successful reconciliation
            self._update_progress(import_batch_id, 
                                f'Reconciliation completed for {month_year}', 
                                90)
            self._add_log(import_batch_id, 
                        f'Reconciliation completed for {month_year} in {minutes}m {seconds}s', 
                        'success')
            app.logger.info(f"Reconciliation completed successfully for month: {month_year} in {minutes}m {seconds}s")
            return {'success': True}
                
        except psycopg2.errors.QueryCanceled:
            error_msg = f"Reconciliation TIMEOUT for {month_year}: Statement exceeded 30 minutes"
            app.logger.error(f"[{import_batch_id}] {error_msg}")
            return {'success': False, 'error': error_msg}
        
        except Exception as e:
            error_msg = f"Reconciliation failed for {month_year}: {str(e)}"
            app.logger.error(f"[{import_batch_id}] {error_msg}")
            return {'success': False, 'error': error_msg}
        finally:
            if conn:
                conn.close()
        
    def _process_files_to_staging(self, import_batch_id, files_to_process, platform):
        """Process files to staging tables - optimized WITH CANCELLATION"""
        try:
            processed_types = set()
            total_files = len(files_to_process)
            
            for i, file_info in enumerate(files_to_process):
                # Check for cancellation before processing each file
                if self.is_cancelled(import_batch_id):
                    app.logger.info(f"[{import_batch_id}] Import cancelled during file processing")
                    return {'success': False, 'cancelled': True, 'error': 'Import cancelled by user'}
                
                file_type = file_info['type']
                file_path = file_info['file_path']
                
                # Update progress less frequently
                progress = 10 + (i / total_files) * 30
                self._update_progress(import_batch_id, f'Importing {file_type.upper()}...', progress)
                
                # Process file
                file_results = process_single_file_optimized(file_path, platform, file_type, import_batch_id)
                
                # Check if file processing was cancelled
                if any('CANCELLED' in result for result in file_results):
                    app.logger.info(f"[{import_batch_id}] File processing cancelled for {file_type}")
                    return {'success': False, 'cancelled': True, 'error': 'Import cancelled by user'}
                
                # Only log errors, not every success
                for result in file_results:
                    if 'ERROR' in result:
                        self._add_log(import_batch_id, result, 'error')
                        return {'success': False, 'error': result}
                    elif 'WARNING' in result:
                        self._add_log(import_batch_id, result, 'warning')
                
                processed_types.add('sales' if file_type in ['b2b', 'b2c'] else 'payout')
                processed_types.add(file_type)
            
            # Single success log instead of multiple
            self._add_log(import_batch_id, f'All {total_files} files processed to staging', 'success')
            return {'success': True, 'file_types': processed_types}
            
        except Exception as e:
            error_msg = f'Staging process failed: {str(e)}'
            self._add_log(import_batch_id, error_msg, 'error')
            return {'success': False, 'error': error_msg}
    
    def _process_final_tables(self, import_batch_id, file_types, platform):
        """Process data from staging to final tables - WITH STAGING CLEANUP"""
        try:
            self._update_progress(import_batch_id, 'Transferring data to final tables...', 50)
            self._add_log(import_batch_id, 'Starting data processing in final tables', 'info')

            # Check for cancellation
            if self.is_cancelled(import_batch_id):
                return {'success': False, 'error': 'Import cancelled by user'}
            
            # Get the actual files that were uploaded in this batch
            current_import = self.active_imports.get(import_batch_id, {})
            uploaded_files = [f['type'] for f in current_import.get('files', [])]
            
            app.logger.info(f"[{import_batch_id}] Uploaded files in this batch: {uploaded_files}")
            
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    
                    # DEBUG: Check staging table counts before processing
                    if any(t in uploaded_files for t in ['b2b', 'b2c']):
                        cur.execute("SELECT COUNT(*) FROM spigen.stg_amz_b2b")
                        b2b_staging_count = cur.fetchone()[0]
                        cur.execute("SELECT COUNT(*) FROM spigen.stg_amz_b2c") 
                        b2c_staging_count = cur.fetchone()[0]
                        app.logger.info(f"[{import_batch_id}] DEBUG - Staging counts - B2B: {b2b_staging_count}, B2C: {b2c_staging_count}")
                    
                    # Process ONLY the sales data types that were actually uploaded
                    sales_uploaded = any(t in uploaded_files for t in ['b2b', 'b2c'])

                    # Check for cancellation
                    if self.is_cancelled(import_batch_id):
                        return {'success': False, 'error': 'Import cancelled by user'}

                     # SET TIMEOUT BEFORE CALLING PROCEDURES
                    cur.execute("SET statement_timeout = 3600000;")  # 60 minutes
                    cur.execute("SET lock_timeout = 3600000;")       # 60 minutes
                    
                    if sales_uploaded:
                        self._add_log(import_batch_id, f'Processing sales data for uploaded files: {[f for f in uploaded_files if f in ["b2b", "b2c"]]}', 'info')
                        
                        try:
                            # Check if this import batch was already processed for sales
                            cur.execute("""
                                SELECT COUNT(*) FROM spigen.tgt_sales 
                                WHERE import_batch_id = %s
                            """, (import_batch_id,))
                            existing_records = cur.fetchone()[0]
                            
                            if existing_records > 0:
                                self._add_log(import_batch_id, f'Sales data already processed for this batch ({existing_records} records), skipping', 'warning')
                            else:
                                # Call procedure with batch_id
                                try:
                                    cur.execute("CALL spigen.process_amazon_sales(%s)", (import_batch_id,))
                                    self._add_log(import_batch_id, 'Sales data processed successfully with batch tracking', 'success')
                                    
                                    # DEBUG: Check if data was actually inserted
                                    cur.execute("SELECT COUNT(*) FROM spigen.tgt_sales WHERE import_batch_id = %s", (import_batch_id,))
                                    inserted_count = cur.fetchone()[0]
                                    app.logger.info(f"[{import_batch_id}] DEBUG - Sales records inserted: {inserted_count}")
                                    
                                except psycopg2.Error as e:
                                    app.logger.warning(f"Sales procedure with batch_id failed: {e}")
                                    self._add_log(import_batch_id, 'Sales processing failed', 'error')
                                    raise
                            
                            # Check for cancellation before cleanup
                            if self.is_cancelled(import_batch_id):
                                conn.rollback()
                                return {'success': False, 'error': 'Import cancelled by user'}
                            
                            # ✅ CLEAR SALES STAGING TABLES AFTER PROCESSING
                            try:
                                if 'b2b' in uploaded_files:
                                    cur.execute("TRUNCATE TABLE spigen.stg_amz_b2b")
                                    app.logger.info(f"[{import_batch_id}] Cleared B2B staging table")
                                
                                if 'b2c' in uploaded_files:
                                    cur.execute("TRUNCATE TABLE spigen.stg_amz_b2c") 
                                    app.logger.info(f"[{import_batch_id}] Cleared B2C staging table")
                                    
                                self._add_log(import_batch_id, 'Sales staging tables cleared successfully', 'info')
                            except Exception as e:
                                app.logger.warning(f"[{import_batch_id}] Failed to clear sales staging tables: {e}")
                        
                        except Exception as e:
                            self._add_log(import_batch_id, f'Sales processing error: {str(e)}', 'error')
                            raise
                    
                    # Check for cancellation before payout processing
                    if self.is_cancelled(import_batch_id):
                        conn.rollback()
                        return {'success': False, 'error': 'Import cancelled by user'}
                    
                    # Process payout data only if it was uploaded in this batch
                    if 'payout' in uploaded_files:
                        self._add_log(import_batch_id, 'Processing payout data in final tables', 'info')
                        try:
                            # Check staging count
                            cur.execute("SELECT COUNT(*) FROM spigen.stg_amz_payout")
                            payout_staging_count = cur.fetchone()[0]
                            app.logger.info(f"[{import_batch_id}] DEBUG - Payout staging count: {payout_staging_count}")
                            
                            # Check if this import batch was already processed for payout
                            cur.execute("""
                                SELECT COUNT(*) FROM spigen.tgt_payout 
                                WHERE import_batch_id = %s
                            """, (import_batch_id,))
                            existing_records = cur.fetchone()[0]
                            
                            if existing_records > 0:
                                self._add_log(import_batch_id, f'Payout data already processed for this batch ({existing_records} records), skipping', 'warning')
                            else:
                                try:
                                    cur.execute("CALL spigen.process_amazon_payout(%s)", (import_batch_id,))
                                    self._add_log(import_batch_id, 'Payout data processed successfully with batch tracking', 'success')
                                    
                                    # DEBUG: Check if data was actually inserted
                                    cur.execute("SELECT COUNT(*) FROM spigen.tgt_payout WHERE import_batch_id = %s", (import_batch_id,))
                                    inserted_count = cur.fetchone()[0]
                                    app.logger.info(f"[{import_batch_id}] DEBUG - Payout records inserted: {inserted_count}")
                                    
                                except psycopg2.Error as e:
                                    app.logger.warning(f"Payout procedure with batch_id failed: {e}")
                                    self._add_log(import_batch_id, 'Payout processing failed', 'error')
                                    raise
                            
                            # Check for cancellation before payout cleanup
                            if self.is_cancelled(import_batch_id):
                                conn.rollback()
                                return {'success': False, 'error': 'Import cancelled by user'}
                                    
                            # ✅ CLEAR PAYOUT STAGING TABLE AFTER PROCESSING
                            try:
                                cur.execute("TRUNCATE TABLE spigen.stg_amz_payout")
                                app.logger.info(f"[{import_batch_id}] Cleared payout staging table")
                                self._add_log(import_batch_id, 'Payout staging table cleared successfully', 'info')
                            except Exception as e:
                                app.logger.warning(f"[{import_batch_id}] Failed to clear payout staging table: {e}")
                        
                        except Exception as e:
                            self._add_log(import_batch_id, f'Payout processing error: {str(e)}', 'error')
                            raise
                    else:
                        self._add_log(import_batch_id, 'No payout file uploaded in this batch, skipping payout processing', 'info')
                    
                    conn.commit()
            
            # Check for cancellation before reconciliation check
            if self.is_cancelled(import_batch_id):
                return {'success': False, 'error': 'Import cancelled by user'}
            
            # ✅ AUTO-TRIGGER RECONCILIATION CHECK AFTER SUCCESSFUL PROCESSING
            # ✅ AUTO-TRIGGER RECONCILIATION CHECK AFTER SUCCESSFUL PROCESSING
            self._update_progress(import_batch_id, 'Checking if reconciliation is needed...', 80)
            reconciliation_result = self.auto_trigger_reconciliation_if_ready(import_batch_id)

            # FIX: Check if reconciliation_result is not None before calling .get()
            if reconciliation_result and reconciliation_result.get('success'):
                self._add_log(import_batch_id, 'RECONCILIATION SUCCESS - Auto-reconciliation completed successfully', 'success')
                self._update_progress(import_batch_id, 'Data processing completed', 90)
                self._add_log(import_batch_id, 'All data processing completed', 'success')
                return {'success': True}
            else:
                reason = reconciliation_result.get('reason', 'Unknown reason') if reconciliation_result else 'No reconciliation result'
                error = reconciliation_result.get('error') if reconciliation_result else None
                
                if error and 'TIMEOUT' in error:
                    self._add_log(import_batch_id, f'RECONCILIATION FAILED: {error}', 'error')
                    return {'success': False, 'error': f'Reconciliation timeout: {error}'}
                else:
                    self._add_log(import_batch_id, f'AUTO-RECONCILIATION SKIPPED: {reason}', 'info')
                    # This is OK - reconciliation wasn't needed, but processing succeeded
                    self._update_progress(import_batch_id, 'Data processing completed', 90)
                    self._add_log(import_batch_id, 'All data processing completed', 'success')
                    return {'success': True}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def should_run_reconciliation(self, import_batch_id):
        """Check for reconciliation - always run for LATEST month with complete data"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Get the LATEST month that has complete data (B2B+B2C+Payout)
            cursor.execute("""
                WITH sales_months AS (
                    -- Get all months that have BOTH B2B and B2C data
                    SELECT DATE_TRUNC('month', invoice_date) as month
                    FROM spigen.tgt_sales 
                    GROUP BY DATE_TRUNC('month', invoice_date)
                    HAVING 
                        COUNT(DISTINCT CASE WHEN channel_id = 1 THEN order_id END) > 0 AND  -- B2B exists
                        COUNT(DISTINCT CASE WHEN channel_id = 2 THEN order_id END) > 0     -- B2C exists
                ),
                payout_months AS (
                    SELECT DATE_TRUNC('month', date) as month
                    FROM spigen.tgt_payout 
                    GROUP BY DATE_TRUNC('month', date)
                ),
                complete_months AS (
                    -- Months that have BOTH sales (B2B+B2C) AND payout data
                    SELECT s.month
                    FROM sales_months s
                    INNER JOIN payout_months p ON s.month = p.month
                ),
                latest_complete_month AS (
                    -- Get the LATEST complete month
                    SELECT MAX(month) as latest_month
                    FROM complete_months
                ),
                reconciliation_status AS (
                    -- Check if latest month is already reconciled
                    SELECT 
                        lcm.latest_month,
                        EXISTS (
                            SELECT 1 FROM spigen.tgt_reconciliation 
                            WHERE reconciliation_month = lcm.latest_month
                            AND last_updated_dttm >= NOW() - INTERVAL '1 hour'
                        ) as already_reconciled
                    FROM latest_complete_month lcm
                )
                SELECT latest_month, already_reconciled
                FROM reconciliation_status
                WHERE latest_month IS NOT NULL
            """)
            
            result = cursor.fetchone()
            
            if result:
                latest_month, already_reconciled = result
                month_year = latest_month.strftime('%Y-%m')
                
                if not already_reconciled:
                    app.logger.info(f"[{import_batch_id}] RECONCILIATION READY for LATEST month: {month_year}")
                    return True, [month_year]
                else:
                    app.logger.info(f"[{import_batch_id}] LATEST MONTH ALREADY RECONCILED: {month_year}")
                    return False, None
            else:
                app.logger.info(f"[{import_batch_id}] NO COMPLETE MONTH FOUND for reconciliation")
                return False, None

        except Exception as e:
            app.logger.error(f"[{import_batch_id}] Error checking reconciliation: {e}")
            return False, None
        finally:
            if conn:
                conn.close()
    
    def auto_trigger_reconciliation_if_ready(self, import_batch_id):
        """Automatically trigger reconciliation ONLY when B2B+B2C+Payout exist for same month"""
        try:
            should_run, reconcilable_months = self.should_run_reconciliation(import_batch_id)
            
            if should_run and reconcilable_months:
                reconciliation_success = True
                reconciled_months = []
                failed_months = []
                
                for month_year in reconcilable_months:
                    app.logger.info(f"[{import_batch_id}] Auto-triggering reconciliation for month: {month_year}")
                    result = self._run_reconciliation_for_month(month_year, import_batch_id)
                    
                    if result.get('success'):
                        app.logger.info(f"[{import_batch_id}] RECONCILIATION SUCCESS for {month_year}")
                        reconciled_months.append(month_year)
                    else:
                        app.logger.warning(f"[{import_batch_id}] RECONCILIATION FAILED for {month_year}: {result.get('error')}")
                        failed_months.append(month_year)
                        reconciliation_success = False
                
                if reconciliation_success:
                    return {'success': True, 'reconciled_months': reconciled_months}
                else:
                    return {'success': False, 'error': f'Reconciliation failed for months: {failed_months}'}
            else:
                current_import = self.active_imports.get(import_batch_id, {})
                uploaded_files = [f['type'] for f in current_import.get('files', [])]
                uploaded_months = current_import.get('uploaded_months', [])
                
                reason = f"Uploaded: {uploaded_files} for months: {uploaded_months} - Need B2B+B2C+Payout for same month"
                app.logger.info(f"[{import_batch_id}] AUTO-RECONCILIATION SKIPPED: {reason}")
                return {'success': False, 'reason': reason}
                
        except Exception as e:
            app.logger.error(f"[{import_batch_id}] Auto-reconciliation check failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _update_progress(self, import_batch_id, step, progress, status='processing'):
        """Update progress for an import - FIXED to include last_update"""
        if import_batch_id in self.active_imports:
            self.active_imports[import_batch_id].update({
                'current_step': step,
                'progress': progress,
                'status': status,
                'last_update': datetime.now()  # CRITICAL: Always update this
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
                        # Clean up cancellation event
                        if import_batch_id in self.cancellation_events:
                            del self.cancellation_events[import_batch_id]
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
        """Get current progress for an import - FIXED VERSION"""
        if import_batch_id in self.active_imports:
            import_data = self.active_imports[import_batch_id]
            # Ensure all required fields are present
            return {
                'status': import_data.get('status', 'unknown'),
                'current_step': import_data.get('current_step', 'Initializing...'),
                'progress': import_data.get('progress', 0),
                'start_time': import_data.get('start_time', datetime.now()),
                'last_update': import_data.get('last_update', import_data.get('start_time', datetime.now())),
                'logs': import_data.get('logs', []),
                'file_types': list(import_data.get('file_types', [])),
                'uploaded_months': list(import_data.get('uploaded_months', [])),
                'files': import_data.get('files', []),
                'files_processed': import_data.get('files_processed', 0),
                'total_files': import_data.get('total_files', 0)
            }
        else:
            return {
                'status': 'not_found',
                'current_step': 'Import not found or completed',
                'progress': 0,
                'start_time': datetime.now(),
                'last_update': datetime.now(),
                'logs': [],
                'file_types': [],
                'uploaded_months': [],
                'files': [],
                'files_processed': 0,
                'total_files': 0
            }

# Initialize progress tracker
progress_tracker = ProgressTracker()

# ---------- OPTIMIZED FILE PROCESSING FUNCTION ----------
def process_single_file_optimized(file_path, platform, table_type, import_batch_id):
    """High-performance file processing with chunked COPY - WITH CANCELLATION CHECKS"""
    table_name = f"stg_{platform.lower()}_{table_type.lower()}"
    results = []
    
    # ✅ Check for cancellation at the very start
    if progress_tracker.is_cancelled(import_batch_id):
        app.logger.info(f"[{import_batch_id}] Import cancelled before processing file: {file_path}")
        return ["CANCELLED - Import was cancelled"]
    
    # ✅ SIMPLE FIX: Extract original filename from file_path
    file_basename = os.path.basename(file_path)
    
    # Remove the import_batch_id and file_type prefix
    parts = file_basename.split('_', 2)
    if len(parts) >= 3:
        original_filename = parts[2]
    else:
        original_filename = file_basename
    
    app.logger.info(f"[{import_batch_id}] Processing {table_type} file: {file_path} (Original: {original_filename})")
    
    try:
        # ✅ CHECK IF FILE ALREADY IMPORTED TO STAGING
        if is_file_imported(original_filename, table_type):
            error_msg = f"ERROR - {table_type.upper()}: File '{original_filename}' has already been imported to staging table"
            results.append(error_msg)
            app.logger.warning(f"[{import_batch_id}] {error_msg}")

            if is_staging_table_empty(table_name):
                app.logger.info(f"[{import_batch_id}] Staging table {table_name} is empty, allowing re-import")
                clear_single_file_tracking(original_filename, table_type)
            else:
                return results
        
        # ✅ Check for cancellation before starting file processing
        if progress_tracker.is_cancelled(import_batch_id):
            app.logger.info(f"[{import_batch_id}] Import cancelled before reading file")
            return ["CANCELLED - Import was cancelled"]
        
        start_time = time.time()
        
        # Read file with optimized parameters
        if table_type.lower() == 'payout':
            df = smart_payout_processor(file_path)
        else:
            df = process_b2b_b2c_file(file_path)
        
        if df.empty:
            return [f"WARNING - {table_type.upper()}: No data found"]

        read_time = time.time()
        app.logger.info(f"[{import_batch_id}] File read in {read_time - start_time:.2f}s, {len(df)} rows")

        # ✅ Check for cancellation after file reading
        if progress_tracker.is_cancelled(import_batch_id):
            app.logger.info(f"[{import_batch_id}] Import cancelled after reading file")
            return ["CANCELLED - Import was cancelled"]

        # Fast column normalization
        df.columns = [col.replace(' ', '_').replace('/', '_').lower() for col in df.columns]
        
        # OPTIMIZATION: Process in chunks for large files
        chunk_results = process_large_file_chunked(df, table_name, table_type, import_batch_id, file_path)

        # ✅ MARK FILE AS IMPORTED ONLY IF SUCCESSFUL AND NOT CANCELLED
        processing_successful = any('SUCCESS' in result for result in chunk_results)
        was_cancelled = any('CANCELLED' in result for result in chunk_results)
        
        if processing_successful and not was_cancelled:
            mark_file_as_imported(original_filename, table_type, import_batch_id)
            app.logger.info(f"[{import_batch_id}] Successfully processed and marked file '{original_filename}' as imported")
        elif was_cancelled:
            app.logger.info(f"[{import_batch_id}] File processing was cancelled: {original_filename}")
        else:
            app.logger.warning(f"[{import_batch_id}] File processing failed, not marking as imported: {chunk_results}")
        
        return chunk_results
    except Exception as e:
        error_msg = f"ERROR - {table_type.upper()}: {str(e)}"
        results.append(error_msg)
        app.logger.error(f"[{import_batch_id}] {error_msg}")
        
    finally:
        # Cleanup temporary file
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                app.logger.warning(f"[{import_batch_id}] Failed to cleanup temp file: {str(e)}")
    
    return results

def is_staging_table_empty(table_name):
    """Check if a staging table is actually empty"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Use the full table name with schema
            full_table_name = f"spigen.{table_name}"
            cur.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            count = cur.fetchone()[0]
            return count == 0
    except Exception as e:
        app.logger.error(f"Error checking if table {table_name} is empty: {e}")
        return True  # If we can't check, assume it's empty to allow processing
    finally:
        if conn:
            conn.close()

def clear_single_file_tracking(filename, file_type):
    """Clear import tracking for a single file"""
    try:
        imported_files = get_imported_files()
        imported_files = [f for f in imported_files if not (f['filename'] == filename and f['type'] == file_type)]
        save_imported_files(imported_files)
        app.logger.info(f"Cleared import tracking for {filename} ({file_type})")
    except Exception as e:
        app.logger.error(f"Error clearing file tracking for {filename}: {e}")
def process_large_file_chunked(df, table_name, table_type, import_batch_id, file_path):
    """SUPER-FAST file processing - processes entire file at once WITH COLUMN MAPPING"""
    try:
        total_rows = len(df)
        
        # ✅ Check for cancellation before starting
        if progress_tracker.is_cancelled(import_batch_id):
            app.logger.info(f"[{import_batch_id}] Import cancelled before processing")
            return ["CANCELLED - Import was cancelled"]
        
        # ✅ CRITICAL FIX: Apply column mapping BEFORE processing
        app.logger.info(f"[{import_batch_id}] Applying column mapping before processing...")
        df = validate_and_align_columns(df, table_name, import_batch_id)
        
        if df.empty:
            app.logger.warning(f"[{import_batch_id}] No data after column alignment")
            return ["WARNING - No valid data found after column alignment"]
        
        app.logger.info(f"[{import_batch_id}] After column mapping: {len(df.columns)} columns, {len(df)} rows")
        
        # ✅ ULTRA-FAST: Process entire file at once (no chunks for files under 50K rows)
        if total_rows <= 50000:
            app.logger.info(f"[{import_batch_id}] Processing {total_rows} rows in SINGLE BATCH")
            return process_entire_file_at_once(df, table_name, table_type, import_batch_id)
        else:
            # For very large files, use optimized chunks
            app.logger.info(f"[{import_batch_id}] Processing {total_rows} rows in OPTIMIZED CHUNKS")
            return process_very_large_file(df, table_name, table_type, import_batch_id)
        
    except Exception as e:
        error_msg = f"ERROR - {table_type.upper()} processing: {str(e)}"
        app.logger.error(f"[{import_batch_id}] {error_msg}")
        return [error_msg]

def process_entire_file_at_once(df, table_name, table_type, import_batch_id):
    """Process entire file in one go - FASTEST method WITH DEBUGGING"""
    try:
        # ✅ DEBUG: Log the columns being inserted
        app.logger.info(f"[{import_batch_id}] DEBUG - Columns to insert: {list(df.columns)}")
        
        conn = get_db_connection()
        
        # TRUNCATE staging table
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE spigen.{table_name}")
            conn.commit()
        
        # Use COPY for maximum speed
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False, na_rep='', quoting=csv.QUOTE_MINIMAL)
        csv_buffer.seek(0)
        
        with conn.cursor() as cur:
            columns_str = ", ".join([f'"{col}"' for col in df.columns])
            
            copy_sql = f"""
                COPY spigen.{table_name} ({columns_str}) 
                FROM STDIN 
                WITH (
                    FORMAT CSV,
                    NULL '',
                    DELIMITER ','
                )
            """
            
            app.logger.info(f"[{import_batch_id}] DEBUG - Executing COPY with columns: {columns_str}")
            
            cur.copy_expert(copy_sql, csv_buffer)
            inserted_rows = cur.rowcount
            conn.commit()
        
        conn.close()
        csv_buffer.close()
        
        result_msg = f"SUCCESS - {table_type.upper()}: {inserted_rows} records processed in SINGLE BATCH"
        app.logger.info(f"[{import_batch_id}] {result_msg}")
        return [result_msg]
        
    except Exception as e:
        error_msg = f"ERROR - {table_type.upper()} single batch: {str(e)}"
        app.logger.error(f"[{import_batch_id}] {error_msg}")
        return [error_msg]

def process_very_large_file(df, table_name, table_type, import_batch_id):
    """Process very large files (>50K rows) with optimized chunks WITH DEBUGGING"""
    try:
        total_rows = len(df)
        chunk_size = 20000  # Larger chunks for better performance
        inserted_rows = 0
        
        # ✅ DEBUG: Log the columns being inserted
        app.logger.info(f"[{import_batch_id}] DEBUG - Columns to insert: {list(df.columns)}")
        
        conn = get_db_connection()
        
        # TRUNCATE staging table once
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE spigen.{table_name}")
            conn.commit()
        
        app.logger.info(f"[{import_batch_id}] Processing {total_rows} rows in chunks of {chunk_size}")
        
        # Process chunks
        for chunk_start in range(0, total_rows, chunk_size):
            if progress_tracker.is_cancelled(import_batch_id):
                app.logger.info(f"[{import_batch_id}] Import cancelled during processing")
                conn.close()
                return ["CANCELLED - Import was cancelled"]
                
            chunk_end = min(chunk_start + chunk_size, total_rows)
            chunk_df = df.iloc[chunk_start:chunk_end]
            
            # Use fast CSV copy for each chunk
            csv_buffer = io.StringIO()
            chunk_df.to_csv(csv_buffer, index=False, header=False, na_rep='', quoting=csv.QUOTE_MINIMAL)
            csv_buffer.seek(0)
            
            with conn.cursor() as cur:
                columns_str = ", ".join([f'"{col}"' for col in df.columns])
                copy_sql = f"COPY spigen.{table_name} ({columns_str}) FROM STDIN WITH (FORMAT CSV, NULL '', DELIMITER ',')"
                cur.copy_expert(copy_sql, csv_buffer)
                inserted_rows += cur.rowcount
            
            csv_buffer.close()
            
            # Progress update every 50K rows
            if chunk_start % 50000 == 0:
                progress = (chunk_end / total_rows) * 100
                app.logger.info(f"[{import_batch_id}] Progress: {progress:.1f}% ({chunk_end}/{total_rows} rows)")
        
        conn.commit()
        conn.close()
        
        result_msg = f"SUCCESS - {table_type.upper()}: {inserted_rows} records processed"
        app.logger.info(f"[{import_batch_id}] {result_msg}")
        return [result_msg]
        
    except Exception as e:
        error_msg = f"ERROR - {table_type.upper()} chunked: {str(e)}"
        app.logger.error(f"[{import_batch_id}] {error_msg}")
        return [error_msg]
def emergency_column_fix(df, table_name, import_batch_id):
    """EMERGENCY FIX for specific column mapping issues"""
    try:
        # Specific fixes for payout files
        if table_name == 'stg_amz_payout':
            column_fixes = {
                'total_sales_tax_liable_gst_before_adjusting_tcs': 'total_sales_tax_liable',
                'total_sales_tax_liable_gst_before_adjusting_tcs_': 'total_sales_tax_liable',
                'total_sales_tax_liable_gst_before_adjusting_tcs__': 'total_sales_tax_liable',
                'total_sales_tax_liable_gst_before_adjusting_tcs___': 'total_sales_tax_liable',
                'total_sales_tax_liable_gst_before_adjusting_tcs____': 'total_sales_tax_liable'
            }
            
            # Apply fixes
            for old_col, new_col in column_fixes.items():
                if old_col in df.columns and new_col not in df.columns:
                    df[new_col] = df[old_col]
                    df = df.drop(columns=[old_col])
                    app.logger.info(f"[{import_batch_id}] EMERGENCY FIX: Renamed {old_col} -> {new_col}")
            
            # Ensure we only have valid columns for the payout table
            valid_payout_columns = [
                'date_time', 'settlement_id', 'type', 'order_id', 'sku', 'description', 'quantity', 
                'marketplace', 'account_type', 'fulfillment', 'order_city', 'order_state', 
                'order_postal', 'product_sales', 'shipping_credits', 'gift_wrap_credits', 
                'promotional_rebates', 'total_sales_tax_liable', 'tcs_cgst', 'tcs_sgst', 'tcs_igst', 
                'tds_section_194_o', 'selling_fees', 'fba_fees', 'other_transaction_fees', 'other', 'total'
            ]
            
            # Keep only valid columns
            columns_to_keep = [col for col in df.columns if col in valid_payout_columns]
            df = df[columns_to_keep]
            
            app.logger.info(f"[{import_batch_id}] EMERGENCY FIX: Final columns: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        app.logger.error(f"[{import_batch_id}] Emergency column fix failed: {e}")
        return df
    
def process_dataframe_chunk_fast(df_chunk, table_name, table_type, import_batch_id):
    """Optimized chunk processing without column validation"""
    if df_chunk.empty:
        return 0
    
    # Create temporary CSV in memory
    csv_buffer = io.StringIO()
    
    # Export with optimized parameters
    df_chunk.to_csv(csv_buffer, index=False, header=False, na_rep='', quoting=csv.QUOTE_MINIMAL)
    csv_buffer.seek(0)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            columns_str = ", ".join([f'"{col}"' for col in df_chunk.columns])
            
            copy_sql = f"""
                COPY spigen.{table_name} ({columns_str}) 
                FROM STDIN 
                WITH (
                    FORMAT CSV,
                    NULL '',
                    DELIMITER ','
                )
            """
            
            cur.copy_expert(copy_sql, csv_buffer)
            inserted_rows = cur.rowcount
            conn.commit()
            
            return inserted_rows
            
    except Exception as e:
        conn.rollback()
        app.logger.error(f"[{import_batch_id}] Chunk insert error: {e}")
        
        # Fallback to executemany for this chunk
        return fallback_chunk_insert_fast(df_chunk, table_name, import_batch_id)
    finally:
        conn.close()
        csv_buffer.close()

def fallback_chunk_insert_fast(df_chunk, table_name, import_batch_id):
    """Optimized fallback method for chunk insertion"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Prepare data tuples - optimized
            data_tuples = []
            for row in df_chunk.itertuples(index=False, name=None):
                processed_row = []
                for value in row:
                    if pd.isna(value) or value == '':
                        processed_row.append(None)
                    elif isinstance(value, (int, float)):
                        processed_row.append(float(value) if pd.notna(value) else None)
                    else:
                        processed_row.append(str(value) if pd.notna(value) else None)
                data_tuples.append(tuple(processed_row))
            
            # Insert with execute_batch for better performance
            columns = [f'"{col}"' for col in df_chunk.columns]
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO spigen.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Use execute_batch with larger page size
            from psycopg2.extras import execute_batch
            execute_batch(cur, insert_sql, data_tuples, page_size=500)  # Increased page size
            
            inserted_rows = len(data_tuples)
            conn.commit()
            return inserted_rows
            
    except Exception as e:
        conn.rollback()
        app.logger.error(f"[{import_batch_id}] Fallback chunk insert failed: {e}")
        return 0
    finally:
        conn.close()


def process_dataframe_chunk(df_chunk, table_name, table_type, import_batch_id):
    """Process a single chunk of dataframe with column validation"""
    
    # Validate and align columns with database schema
    df_chunk = validate_and_align_columns(df_chunk, table_name, import_batch_id)
    
    if df_chunk.empty:
        app.logger.warning(f"[{import_batch_id}] No data after column alignment")
        return 0
    
    # Create temporary CSV in memory
    csv_buffer = io.StringIO()
    
    # Export with optimized parameters
    df_chunk.to_csv(csv_buffer, index=False, header=False, na_rep='')
    csv_buffer.seek(0)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            columns_str = ", ".join([f'"{col}"' for col in df_chunk.columns])
            
            copy_sql = f"""
                COPY spigen.{table_name} ({columns_str}) 
                FROM STDIN 
                WITH (
                    FORMAT CSV,
                    NULL '',
                    DELIMITER ','
                )
            """
            
            cur.copy_expert(copy_sql, csv_buffer)
            inserted_rows = cur.rowcount
            conn.commit()
            
            app.logger.info(f"[{import_batch_id}] Successfully inserted {inserted_rows} rows")
            return inserted_rows
            
    except Exception as e:
        conn.rollback()
        app.logger.error(f"[{import_batch_id}] Chunk insert error: {e}")
        
        # Fallback to executemany for this chunk
        return fallback_chunk_insert(df_chunk, table_name, import_batch_id)
    finally:
        conn.close()
        csv_buffer.close()

def fallback_chunk_insert(df_chunk, table_name, import_batch_id):
    """Fallback method for chunk insertion"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Prepare data tuples
            data_tuples = []
            for row in df_chunk.itertuples(index=False, name=None):
                processed_row = []
                for value in row:
                    if pd.isna(value) or value == '':
                        processed_row.append(None)
                    elif isinstance(value, (int, float)):
                        processed_row.append(float(value) if pd.notna(value) else None)
                    else:
                        processed_row.append(str(value) if pd.notna(value) else None)
                data_tuples.append(tuple(processed_row))
            
            # Insert with executemany
            columns = [f'"{col}"' for col in df_chunk.columns]
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO spigen.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Use execute_batch for better performance
            from psycopg2.extras import execute_batch
            execute_batch(cur, insert_sql, data_tuples, page_size=100)
            
            inserted_rows = len(data_tuples)
            conn.commit()
            return inserted_rows
            
    except Exception as e:
        conn.rollback()
        app.logger.error(f"[{import_batch_id}] Fallback chunk insert failed: {e}")
        return 0
    finally:
        conn.close()

def process_small_file_direct(df, table_name, table_type, import_batch_id, file_path):
    """Process small files directly (original method optimized)"""
    try:
        # Use memory buffer instead of temporary file
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False, na_rep='')
        csv_buffer.seek(0)
        
        conn = get_db_connection()
        with conn.cursor() as cur:
            # TRUNCATE staging table
            cur.execute(f"TRUNCATE TABLE spigen.{table_name}")
            
            # COPY from memory buffer
            columns_str = ", ".join([f'"{col}"' for col in df.columns])
            copy_sql = f"""
                COPY spigen.{table_name} ({columns_str}) 
                FROM STDIN 
                WITH (
                    FORMAT CSV,
                    NULL '',
                    DELIMITER ','
                )
            """
            
            cur.copy_expert(copy_sql, csv_buffer)
            inserted_rows = cur.rowcount
            conn.commit()
        
        conn.close()
        csv_buffer.close()
        
        result_msg = f"SUCCESS - {table_type.upper()}: {inserted_rows} records"
        return [result_msg]
        
    except Exception as e:
        error_msg = f"ERROR - {table_type.upper()}: {str(e)}"
        app.logger.error(f"[{import_batch_id}] {error_msg}")
        return [error_msg]

def try_alternative_insert(df, table_name, import_batch_id, original_error):
    """Alternative approach using fast executemany"""
    app.logger.info(f"[{import_batch_id}] Trying alternative INSERT approach...")
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # TRUNCATE first
            cur.execute(f"TRUNCATE TABLE spigen.{table_name}")
            
            # Prepare data - convert all values to proper types
            data_tuples = []
            for row in df.itertuples(index=False, name=None):
                processed_row = []
                for value in row:
                    if pd.isna(value) or value == '':
                        processed_row.append(None)
                    elif isinstance(value, (int, float)):
                        processed_row.append(float(value) if pd.notna(value) else None)
                    else:
                        processed_row.append(str(value) if pd.notna(value) else None)
                data_tuples.append(tuple(processed_row))
            
            # Insert in small chunks to avoid timeouts
            chunk_size = 500
            inserted_rows = 0
            
            columns = [f'"{col}"' for col in df.columns]
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO spigen.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            for i in range(0, len(data_tuples), chunk_size):
                chunk = data_tuples[i:i + chunk_size]
                cur.executemany(insert_sql, chunk)
                inserted_rows += len(chunk)
                app.logger.info(f"[{import_batch_id}] Inserted chunk {i//chunk_size + 1}")
            
            conn.commit()
            return [f"SUCCESS - {table_name.split('_')[-1].upper()}: {inserted_rows} records (alternative method)"]
            
    except Exception as e:
        conn.rollback()
        return [f"ERROR - {table_name.split('_')[-1].upper()}: Both methods failed. Original: {original_error}, Alternative: {e}"]
    finally:
        conn.close()
        
def optimize_dataframe_fast(df, table_name):
    """Faster dataframe optimization"""
    # Only process essential columns
    essential_date_cols = ['invoice_date', 'date_time']
    existing_dates = [col for col in essential_date_cols if col in df.columns]
    
    for date_col in existing_dates:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Fast numeric processing for critical columns only
    numeric_cols = [col for col in df.columns if any(pattern in col for pattern in ['amount', 'total', 'sales', 'tax'])]
    for col in numeric_cols:
        if col in df.columns:
            # Fast numeric conversion
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

def clear_db_schema_cache():
    """Clear the database schema cache"""
    global DB_SCHEMA_CACHE
    DB_SCHEMA_CACHE.clear()
    app.logger.info("Database schema cache cleared")

# You can call this function when you know the database schema has changed
@app.route('/admin/clear-cache', methods=['POST'])
def admin_clear_cache():
    """Admin endpoint to clear cache"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    clear_db_schema_cache()
    flash("Database schema cache cleared", "success")
    return redirect(url_for('admin_import_tracking'))

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
        
        if username in VALID_USERS and VALID_USERS[username] == password:
            session['user'] = username
            flash('Login successful!', 'success')
            return redirect(url_for('upload'))
        else:
            flash('Invalid credentials. Try again.', "danger")
    return render_template('login.html')

# ---------- ERROR HANDLERS ----------
@app.errorhandler(413)
def too_large(e):
    """Handle file too large errors with better messaging"""
    app.logger.error(f"File upload too large: {e}")
    flash("File too large. Maximum total upload size is 500MB. Please upload smaller files or split your data.", "danger")
    return redirect(url_for('upload'))

@app.errorhandler(500)
def internal_error(e):
    """Handle internal server errors"""
    app.logger.error(f"Internal server error: {e}")
    flash("An internal error occurred. Please try again with smaller files.", "danger")
    return redirect(url_for('upload'))

@app.route('/logout')
def logout():
    """Logout user"""
    session.pop('user', None)
    flash('You have been logged out', 'info')
    return redirect(url_for('home'))

# ---------- MAIN UPLOAD ROUTE (FIXED) ----------
@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if 'user' not in session:
        flash("Please login first", "warning")
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        try:
            # Generate request fingerprint for deduplication
            fingerprint = request_deduplicator.generate_request_fingerprint(request, session)
            
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
            import_batch_id = str(uuid.uuid4()).replace('-', '')[:8]
            
            # ✅ CHECK FILES BEFORE PROCESSING - PREVENT DUPLICATE UPLOADS
            files_to_check = [
                ('b2b_file', 'b2b'),
                ('b2c_file', 'b2c'), 
                ('payout_file', 'payout')
            ]
            
            blocked_files = []
            
            for file_field, file_type in files_to_check:
                file = request.files.get(file_field)
                if file and file.filename:
                    filename = secure_filename(file.filename)
                    
                    # ✅ Check if file already imported to staging
                    if is_file_imported(filename, file_type):
                        blocked_files.append({
                            'filename': filename,
                            'type': file_type,
                            'reason': 'already imported to staging table'
                        })
                        app.logger.warning(f"File '{filename}' already imported to staging, blocking upload")
                        continue
                    
                    # ✅ Check for duplicate files in current session
                    duplicate_in_session = any(
                        f['filename'] == filename and f['type'] == file_type 
                        for f in files_to_process
                    )
                    if duplicate_in_session:
                        blocked_files.append({
                            'filename': filename,
                            'type': file_type,
                            'reason': 'duplicate in current upload'
                        })
                        app.logger.warning(f"File '{filename}' duplicate in current session, blocking upload")
                        continue
                    
                    # ✅ Save file for processing
                    file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{import_batch_id}_{file_type}_{filename}")
                    file.save(file_path)
                    files_to_process.append({
                        'type': file_type,
                        'file_path': file_path,
                        'filename': filename,  # This is the original filename
                        'original_filename': filename  #Add this line explicitly
                        
                    })
                    app.logger.info(f"File '{filename}' accepted for processing")
            
            # ✅ Handle case where no files can be processed
            if not files_to_process:
                if blocked_files:
                    blocked_names = [f"{f['filename']} ({f['type'].upper()})" for f in blocked_files]
                    flash(f"All files blocked from upload: {', '.join(blocked_names)}. Files already imported to staging tables cannot be uploaded again.", "warning")
                else:
                    flash("Please select at least one file to upload", "danger")
                return redirect(url_for('upload'))
            
            # ✅ Show warning if some files were blocked
            if blocked_files:
                blocked_names = [f"{f['filename']} ({f['type'].upper()}) - {f['reason']}" for f in blocked_files]
                flash(f"Some files were skipped: {', '.join(blocked_names)}", "warning")
            
            # Start the import process
            progress_tracker.start_import_process(import_batch_id, files_to_process, platform)
            
            # Mark request as completed in deduplication
            request_deduplicator.complete_request(fingerprint)
            
            # ✅ Show success message with accepted files
            accepted_files = [f"{f['filename']} ({f['type'].upper()})" for f in files_to_process]
            flash(f"Import started successfully! Processing: {', '.join(accepted_files)} | Tracking ID: {import_batch_id}", "success")
            
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
    
    # Get import data for display
    import_data = None
    if import_id:
        import_data = progress_tracker.get_progress(import_id)
    
    return render_template('import_progress.html', 
                         import_id=import_id,
                         import_data=import_data,
                         active_imports=list(progress_tracker.active_imports.keys()),
                         now=datetime.now())
def check_database_performance():
    """Check if required indexes exist and provide optimization suggestions"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            
            # Check for critical indexes
            cur.execute("""
                SELECT 
                    tablename,
                    indexname,
                    indexdef
                FROM pg_indexes 
                WHERE schemaname = 'spigen'
                AND tablename IN ('tgt_sales', 'tgt_payout', 'tgt_reconciliation', 'unmatched_orders')
                ORDER BY tablename, indexname;
            """)
            
            existing_indexes = cur.fetchall()
            app.logger.info("Existing indexes for reconciliation:")
            for index in existing_indexes:
                app.logger.info(f"  {index[0]}.{index[1]}")
            
            # Check table sizes
            cur.execute("""
                SELECT 
                    table_name,
                    PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE('spigen.' || table_name)) as size
                FROM information_schema.tables 
                WHERE table_schema = 'spigen'
                AND table_name IN ('tgt_sales', 'tgt_payout')
            """)
            
            table_sizes = cur.fetchall()
            app.logger.info("Table sizes:")
            for table, size in table_sizes:
                app.logger.info(f"  {table}: {size}")
                
        return existing_indexes, table_sizes
        
    except Exception as e:
        app.logger.error(f"Error checking database performance: {e}")
        return [], []
    finally:
        if conn:
            conn.close()

# Call this during startup
# Call this during startup - FIXED for newer Flask versions
@app.before_request
def initialize_performance_checks():
    """Run performance checks on first request"""
    # Check if we've already initialized
    if not hasattr(app, 'performance_checks_done'):
        indexes, sizes = check_database_performance()
        app.logger.info("Database performance initialization completed")
        app.performance_checks_done = True

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
        with get_db_connection() as conn:
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

# ---------- FILE IMPORT TRACKING ----------
IMPORT_TRACKER_FILE = 'imported_files.json'

def get_imported_files():
    """Get list of imported files from tracker"""
    try:
        if os.path.exists(IMPORT_TRACKER_FILE):
            with open(IMPORT_TRACKER_FILE, 'r') as f:
                return json.load(f)
    except:
        pass
    return []

def save_imported_files(files):
    """Save imported files to tracker"""
    try:
        with open(IMPORT_TRACKER_FILE, 'w') as f:
            json.dump(files, f, indent=2)
    except Exception as e:
        app.logger.error(f"Error saving imported files: {e}")

def mark_file_as_imported(filename, file_type, import_id):
    """Mark a file as imported to staging table"""
    imported_files = get_imported_files()
    
    # Check if file already exists
    existing_file = next((f for f in imported_files if f['filename'] == filename and f['type'] == file_type), None)
    
    if existing_file:
        # Update existing entry
        existing_file['last_imported'] = datetime.now().isoformat()
        existing_file['import_id'] = import_id
        existing_file['status'] = 'staging_imported'
    else:
        # Add new entry
        imported_files.append({
            'filename': filename,
            'type': file_type,
            'import_id': import_id,
            'first_imported': datetime.now().isoformat(),
            'last_imported': datetime.now().isoformat(),
            'status': 'staging_imported',
            'staging_table': f"stg_amz_{file_type}" if file_type in ['b2b', 'b2c'] else f"stg_amz_payout"
        })
    
    save_imported_files(imported_files)

def is_file_imported(filename, file_type):
    """Check if file has been imported to staging"""
    imported_files = get_imported_files()
    return any(f['filename'] == filename and f['type'] == file_type for f in imported_files)

# Add these API routes to your Flask app
@app.route('/api/check-staging-import', methods=['GET'])
def check_staging_import():
    """API endpoint to check if file is already imported to staging"""
    filename = request.args.get('filename')
    file_type = request.args.get('type')
    
    if not filename or not file_type:
        return jsonify({'error': 'Missing filename or type'}), 400
    
    is_imported = is_file_imported(filename, file_type)
    
    return jsonify({
        'filename': filename,
        'type': file_type,
        'imported': is_imported
    })

@app.route('/api/mark-file-imported', methods=['POST'])
def mark_imported():
    """API endpoint to mark file as imported"""
    data = request.get_json()
    filename = data.get('filename')
    file_type = data.get('type')
    import_id = data.get('import_id')
    
    if not filename or not file_type or not import_id:
        return jsonify({'error': 'Missing required fields'}), 400
    
    mark_file_as_imported(filename, file_type, import_id)
    
    return jsonify({
        'success': True,
        'message': f'File {filename} marked as imported'
    })

# ---------- API ROUTES (FIXED) ----------
@app.route('/api/recent-logs')
def api_recent_logs():
    """API endpoint to get recent logs for the upload page"""
    try:
        # Get recent logs from database
        with get_db_connection() as conn:
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

@app.route('/api/import-status')
def api_import_status():
    """API endpoint to get current import status - FIXED VERSION"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    import_id = request.args.get('import_id')
    
    app.logger.info(f"DEBUG: Import status requested for ID: {import_id}")
    app.logger.info(f"DEBUG: Active imports: {list(progress_tracker.active_imports.keys())}")
    
    # If specific import ID is provided, return only that
    if import_id:
        import_data = progress_tracker.get_progress(import_id)
        
        # Enhanced response with all required fields
        response_data = {
            'import_id': import_id,
            'status': import_data.get('status', 'unknown'),
            'current_step': import_data.get('current_step', 'Initializing...'),
            'progress': import_data.get('progress', 0),
            'start_time': import_data.get('start_time', datetime.now()).isoformat(),
            'last_update': import_data.get('last_update', datetime.now()).isoformat(),
            'logs': import_data.get('logs', [])[-20:],  # Last 20 logs
            'file_types': list(import_data.get('file_types', [])),
            'uploaded_months': list(import_data.get('uploaded_months', [])),
            'files': [{'filename': f.get('filename', 'Unknown'), 'type': f.get('type', 'unknown')} 
                     for f in import_data.get('files', [])],
            'files_processed': import_data.get('files_processed', 0),
            'total_files': import_data.get('total_files', 0)
        }
        
        app.logger.info(f"DEBUG: Returning import data for {import_id}: status={response_data['status']}, progress={response_data['progress']}")
        
        return jsonify(response_data)
    
    # Otherwise return all active imports (for admin view)
    active_imports = {}
    for import_id, import_data in progress_tracker.active_imports.items():
        active_imports[import_id] = {
            'status': import_data.get('status', 'unknown'),
            'current_step': import_data.get('current_step', 'Unknown step'),
            'progress': import_data.get('progress', 0),
            'start_time': import_data.get('start_time', datetime.now()).isoformat(),
            'last_update': import_data.get('last_update', datetime.now()).isoformat(),
                        'file_types': list(import_data.get('file_types', [])),
            'uploaded_months': list(import_data.get('uploaded_months', []))
        }
    
    return jsonify({
        'active_imports': active_imports,
        'total_active': len(active_imports)
    })

@app.route('/api/cancel-import', methods=['POST'])
def cancel_import():
    """API endpoint to cancel an import - FIXED VERSION"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json()
    import_id = data.get('import_id')

    if not import_id:
        return jsonify({'error': 'Missing import_id'}), 400

    try:
        # Request cancellation - this will actually stop the process
        success = progress_tracker.request_cancellation(import_id)
        
        if success:
            app.logger.info(f"[{import_id}] Import cancellation initiated successfully")
            return jsonify({
                'success': True,
                'message': 'Import cancellation initiated. Process will stop shortly.'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Import cannot be cancelled in its current state'
            }), 400

    except Exception as e:
        app.logger.error(f"Error cancelling import {import_id}: {e}")
        return jsonify({'error': f'Failed to cancel import: {str(e)}'}), 500
        
@app.route('/admin/import-tracking')
def admin_import_tracking():
    """Admin page to view and manage import tracking"""
    if 'user' not in session:
        flash("Please login first", "warning")
        return redirect(url_for('login'))
    
    imported_files = get_imported_files()
    
    return render_template('admin_tracking.html', 
                         imported_files=imported_files,
                         total_files=len(imported_files))

@app.route('/admin/clear-all-tracking', methods=['POST'])
def clear_all_tracking():
    """Clear all import tracking (admin only)"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        save_imported_files([])
        app.logger.info("All import tracking cleared by admin")
        flash("All import tracking has been cleared", "success")
        return redirect(url_for('admin_import_tracking'))
    except Exception as e:
        flash(f"Error clearing tracking: {e}", "danger")
        return redirect(url_for('admin_import_tracking'))

@app.route('/api/retry-import', methods=['POST'])
def retry_import():
    """API endpoint to retry a failed import"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.get_json()
    import_id = data.get('import_id')
    
    if not import_id:
        return jsonify({'error': 'Missing import_id'}), 400
    
    # For now, return error - implement actual retry logic
    return jsonify({
        'success': False,
        'error': 'Retry functionality not implemented yet'
    })

@app.route('/api/resume-import', methods=['POST'])
def resume_import():
    """API endpoint to resume an interrupted import"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.get_json()
    import_id = data.get('import_id')
    
    if not import_id:
        return jsonify({'error': 'Missing import_id'}), 400
    
    # For now, return error - implement actual resume logic
    return jsonify({
        'success': False,
        'error': 'Resume functionality not implemented yet'
    })

@app.route('/api/health')
def health_check():
    """Health check endpoint for backend availability"""
    try:
        # Simple database check
        conn = get_db_connection()
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'database': 'disconnected', 'error': str(e)}), 500

# ---------- UTILITY ROUTES ----------
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
    
@app.route('/api/clear-import-tracking', methods=['POST'])
def clear_import_tracking():
    """API endpoint to clear import tracking (for admin/testing)"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.get_json()
    filename = data.get('filename')
    file_type = data.get('type')
    
    try:
        imported_files = get_imported_files()
        
        if filename and file_type:
            # Remove specific file
            imported_files = [f for f in imported_files if not (f['filename'] == filename and f['type'] == file_type)]
            message = f"Cleared import tracking for {filename} ({file_type})"
        else:
            # Clear all tracking
            imported_files = []
            message = "Cleared all import tracking"
        
        save_imported_files(imported_files)
        app.logger.info(f"Import tracking cleared: {message}")
        
        return jsonify({
            'success': True,
            'message': message
        })
        
    except Exception as e:
        app.logger.error(f"Error clearing import tracking: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/reset-session-tracking', methods=['POST'])
def reset_session_tracking():
    """API endpoint to reset session-based file blocking"""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    # This would typically be handled by the frontend clearing sessionStorage
    # But we can clear any server-side session tracking here
    return jsonify({
        'success': True,
        'message': 'Session tracking can be reset from frontend'
    })

# ---------- MISSING UTILITY FUNCTIONS ----------
def optimized_date_parser(date_series):
    """Optimized date parser that handles Amazon-specific formats"""
    return enhanced_date_parser(date_series)

def process_b2b_b2c_file(filepath):
    """Optimized processor for B2B/B2C files"""
    print(f"\nProcessing B2B/B2C file: {filepath}")
    
    # For large files, use chunksize
    if os.path.getsize(filepath) > 10 * 1024 * 1024:  # 10MB
        return read_large_csv_optimized(filepath)
    
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            if filepath.endswith('.csv'):
                # OPTIMIZED: Use faster parameters
                df = pd.read_csv(
                    filepath, 
                    encoding=encoding, 
                    thousands=',',
                    keep_default_na=True,
                    na_values=['', 'NULL', 'null', 'N/A', 'n/a'],
                    low_memory=False,  # Important for large files
                    engine='c'  # C engine is faster
                )
            else:
                df = pd.read_excel(
                    filepath, 
                    keep_default_na=True, 
                    na_values=['', 'NULL', 'null', 'N/A', 'n/a']
                )
            print(f"Successfully read with encoding: {encoding}")
            return df
        except Exception as e:
            print(f"Failed with encoding {encoding}: {e}")
            continue
    
    # Final fallback
    try:
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='utf-8', errors='replace', thousands=',', low_memory=False)
        else:
            df = pd.read_excel(filepath)
        return df
    except Exception as e:
        raise Exception(f"Could not read file: {str(e)}")

def read_large_csv_optimized(filepath):
    """Read large CSV files efficiently"""
    chunks = []
    chunk_size = 50000
    
    for chunk in pd.read_csv(
        filepath, 
        chunksize=chunk_size,
        encoding='utf-8',
        thousands=',',
        low_memory=False,
        engine='c'
    ):
        chunks.append(chunk)
    
    return pd.concat(chunks, ignore_index=True)

def smart_payout_processor(filepath):
    """
    Robust processor for Amazon payout files with PROPER header detection
    """
    print(f"\nProcessing payout file: {filepath}")

    # Step 1: Read raw file text
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

    # Step 2: BETTER Header detection - skip description lines
    lines = raw_text.splitlines()
    header_index = None
    
    # Skip initial description/empty lines and find the REAL header
    for i, line in enumerate(lines):
        line_clean = line.strip()
        if not line_clean or line_clean.startswith('"') or 'includes' in line.lower():
            continue  # Skip empty lines and description lines
        
        # Look for the actual column header line
        line_lower = line.lower()
        header_keywords = ['date/time', 'settlement id', 'type', 'order id', 'sku', 'description', 'quantity', 'marketplace']
        
        keyword_matches = sum(1 for keyword in header_keywords if keyword in line_lower)
        
        if keyword_matches >= 4:  # Require at least 4 matching columns to be sure
            header_index = i
            print(f"REAL Header found at line {i+1} with {keyword_matches} matching columns")
            print(f"Header line: {line}")
            break
    
    if header_index is None:
        # Emergency: look for any line with many commas (actual data header)
        for i, line in enumerate(lines):
            if line.count(',') > 10:  # Real header should have many columns
                header_index = i
                print(f"Emergency header detection at line {i+1} based on comma count")
                print(f"Header line: {line}")
                break
    
    if header_index is None:
        # Last resort: use pandas auto-detection
        print("Using pandas auto header detection")
        try:
            df = pd.read_csv(filepath, encoding=used_encoding, thousands=',', nrows=0)
            header_index = 0  # Assume first row is header
            print(f"Pandas detected columns: {list(df.columns)}")
        except:
            raise Exception("Could not find header line in file")

    # Step 3: CORRECT Delimiter detection from ACTUAL header line
    header_line = lines[header_index]
    print(f"Analyzing header line for delimiter: {header_line}")
    
    # Count delimiters in the actual header
    possible_delims = [',', '\t', ';', '|']
    delim_counts = {d: header_line.count(d) for d in possible_delims}
    print(f"Delimiter counts: {delim_counts}")
    
    best_delim = max(delim_counts, key=delim_counts.get)
    print(f"Selected delimiter: '{best_delim}' with {delim_counts[best_delim]} occurrences")

    # Step 4: Read with CORRECT parameters
    try:
        print(f"Reading CSV with: skiprows={header_index}, delimiter='{best_delim}'")
        df = pd.read_csv(
            io.StringIO(raw_text),
            skiprows=header_index,
            delimiter=best_delim,
            engine='python',
            on_bad_lines='skip',
            thousands=',',
            keep_default_na=True,
            na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NaN', 'None'],
            quotechar='"',
            skipinitialspace=True
        )
        print(f"Successfully read CSV with {len(df.columns)} columns, {len(df)} rows")
        
    except Exception as e:
        print(f"Standard read failed: {e}, trying alternative approach")
        # Try with different parameters
        df = pd.read_csv(
            io.StringIO(raw_text),
            skiprows=header_index,
            sep=best_delim,
            engine='python',
            error_bad_lines=False,
            warn_bad_lines=True,
            thousands=','
        )

    # Step 5: Handle if we still get single column (emergency split)
    if df.shape[1] == 1:
        print("EMERGENCY: Single column detected, manual splitting required")
        print(f"First few rows: {df.head(3).iloc[:, 0].tolist()}")
        
        # Manual split on the detected delimiter
        split_data = []
        for line in lines[header_index + 1:]:  # Skip header
            if line.strip():
                split_row = line.split(best_delim)
                split_data.append(split_row)
        
        if split_data:
            # Create dataframe from split data
            df = pd.DataFrame(split_data)
            # Use header line for column names
            header_columns = lines[header_index].split(best_delim)
            if len(header_columns) == df.shape[1]:
                df.columns = [normalize_column_name(col) for col in header_columns]
                print(f"Manual split successful: {len(df.columns)} columns, {len(df)} rows")
            else:
                print(f"Column count mismatch: header has {len(header_columns)}, data has {df.shape[1]}")
                # Create generic column names
                df.columns = [f'col_{i}' for i in range(df.shape[1])]

    # Step 6: Enhanced Column Normalization
    def normalize_column_name(col):
        """Normalize column names to be database-safe"""
        if pd.isna(col):
            return "unknown_column"
        
        col = str(col).strip()
        # Remove quotes and extra spaces
        col = col.replace('"', '').replace("'", "")
        col = re.sub(r'\s+', ' ', col)  # Collapse multiple spaces
        
        # Replace problematic characters
        col = col.replace(' ', '_')
        col = col.replace('/', '_')
        col = col.replace('(', '_')
        col = col.replace(')', '_')
        col = col.replace('-', '_')
        col = col.replace('.', '_')
        col = col.replace('&', 'and')
        col = col.replace('%', 'percent')
        col = col.replace('$', 'usd')
        col = col.replace('#', 'number')
        
        # Remove multiple underscores and trailing/leading underscores
        col = re.sub(r'_+', '_', col)
        col = col.strip('_')
        
        # Ensure it starts with a letter
        if col and not col[0].isalpha():
            col = 'col_' + col
            
        # Handle empty column names
        if not col:
            return "unknown_column"
            
        return col.lower()

    df.columns = [normalize_column_name(col) for col in df.columns]
    
    print(f"Final normalized columns: {list(df.columns)}")
    
    # Step 7: IMPROVED Date/Time parsing with better debugging
    date_columns = ['date_time', 'date_time', 'date', 'time', 'transaction_date', 'date/time']
    
    for date_col in date_columns:
        if date_col in df.columns:
            print(f"Processing date column: {date_col}")
            sample_values = df[date_col].head(5).dropna()
            if not sample_values.empty:
                print(f"Sample date values before parsing: {sample_values.tolist()}")
            
            # Parse dates
            df[date_col] = enhanced_date_parser(df[date_col])
            
            # Check results
            parsed_sample = df[date_col].head(5).dropna()
            if not parsed_sample.empty:
                print(f"Sample date values after parsing: {parsed_sample.tolist()}")
                print(f"Successfully parsed {df[date_col].notna().sum()}/{len(df)} dates")
            break
    
    
    # Step 8: Clean numeric columns
    numeric_cols = [
        'product_sales', 'shipping_credits', 'gift_wrap_credits', 'promotional_rebates',
        'total_sales_tax_liable', 'tcs_cgst', 'tcs_sgst', 'tcs_igst', 'tds_section_194_o',
        'selling_fees', 'fba_fees', 'other_transaction_fees', 'other', 'total', 'quantity'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            print(f"Cleaning numeric column: {col}")
            df[col] = (
                df[col].astype(str)
                .str.replace(',', '', regex=False)
                .str.replace('₹', '', regex=False)
                .str.replace('$', '', regex=False)
                .str.replace(r'[^\d\.-]', '', regex=True)
                .replace(['', 'nan', 'none', 'null', 'NaN', 'N/A', 'n/a'], '0', regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    print(f"Final result: {len(df.columns)} columns, {len(df)} rows")
    return df

def enhanced_date_parser(date_series):
    """Enhanced date parser with Amazon-specific formats - FIXED for single-digit dates"""
    if date_series.empty:
        return date_series
    
    # Convert to string and clean
    date_str = date_series.astype(str).str.strip()
    
    # Amazon-specific date formats in priority order - UPDATED FORMATS
    formats = [
        '%d %b %Y %H:%M:%S',         # 1 Sept 2024 1:38:00 (FIXED - no AM/PM)
        '%d %B %Y %H:%M:%S',         # 1 September 2024 1:38:00
        '%d %b %Y %I:%M:%S %p',      # 1 Sept 2024 1:38:00 PM (with AM/PM)
        '%d %B %Y %I:%M:%S %p',      # 1 September 2024 1:38:00 PM
        '%d %b %Y %I:%M:%S %p UTC',  # 1 Sept 2024 1:38:00 PM UTC
        '%d %B %Y %I:%M:%S %p UTC',  # 1 September 2024 1:38:00 PM UTC
        '%Y-%m-%d %H:%M:%S',         # 2024-09-01 13:38:00
        '%d-%m-%Y %H:%M:%S',         # 01-09-2024 13:38:00
        '%m/%d/%Y %H:%M:%S',         # 09/01/2024 13:38:00
        '%d/%m/%Y %H:%M:%S',         # 01/09/2024 13:38:00
        '%Y-%m-%d',                  # 2024-09-01
        '%d-%m-%Y',                  # 01-09-2024
    ]
    
    # Try each format
    for fmt in formats:
        try:
            parsed = pd.to_datetime(date_str, format=fmt, errors='coerce')
            success_count = parsed.notna().sum()
            success_rate = success_count / len(parsed)
            print(f"Date format '{fmt}': {success_count}/{len(parsed)} successful ({success_rate:.1%})")
            
            if success_rate > 0.8:  # 80% success threshold
                print(f"✅ Using date format: {fmt}")
                return parsed
        except Exception as e:
            print(f"Format {fmt} failed: {e}")
            continue
    
    # Special handling for Amazon format with single-digit dates and times
    print("🔄 Trying custom Amazon date parsing for single-digit formats...")
    try:
        def parse_single_digit_amazon_date(date_str):
            try:
                # Handle formats like: "1 Sept 2024 1:38:00" or "1 Sept 2024 1:38:00 PM"
                if pd.isna(date_str) or date_str == 'nan' or date_str == '':
                    return pd.NaT
                    
                date_str = str(date_str).strip()
                
                # Split into components
                parts = date_str.split()
                if len(parts) < 4:
                    return pd.NaT
                
                # Extract day, month, year
                day = parts[0]
                month = parts[1]
                year = parts[2]
                time_part = parts[3]
                
                # Handle AM/PM if present
                am_pm = None
                if len(parts) > 4:
                    am_pm = parts[4].upper()
                
                # Parse time component
                time_parts = time_part.split(':')
                if len(time_parts) != 3:
                    return pd.NaT
                
                hour = int(time_parts[0])
                minute = int(time_parts[1])
                second = int(time_parts[2])
                
                # Convert to 24-hour format if AM/PM is present
                if am_pm:
                    if am_pm == 'PM' and hour < 12:
                        hour += 12
                    elif am_pm == 'AM' and hour == 12:
                        hour = 0
                
                # Create datetime object
                from datetime import datetime
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12,
                    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
                    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
                }
                
                month_num = month_map.get(month.lower(), 1)
                
                return datetime(int(year), month_num, int(day), hour, minute, second)
                
            except Exception as e:
                print(f"Custom parser failed for '{date_str}': {e}")
                return pd.NaT
        
        parsed = date_str.apply(parse_single_digit_amazon_date)
        success_count = parsed.notna().sum()
        if success_count > 0:
            print(f"✅ Custom single-digit parser: {success_count}/{len(parsed)} successful")
            return parsed
    except Exception as e:
        print(f"Custom single-digit parser failed: {e}")
    
    # Final attempt with dayfirst=True for European/Indian formats
    print("⚠️ Using dayfirst=True as last resort")
    try:
        parsed = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
        success_count = parsed.notna().sum()
        if success_count > 0:
            print(f"✅ Dayfirst parsing: {success_count}/{len(parsed)} successful")
            return parsed
    except Exception as e:
        print(f"Dayfirst parsing failed: {e}")
    
    # Ultimate fallback
    print("🚨 Using flexible parsing as ultimate fallback")
    return pd.to_datetime(date_str, errors='coerce')


# Add this cache at the top of your file, after imports
DB_SCHEMA_CACHE = {}
DB_SCHEMA_CACHE_TIMEOUT = 3600  # 5 minutes
def validate_and_align_columns(df, table_name, import_batch_id):
    """ULTRA-FAST column validation - SKIPS database lookup for known schemas"""
    try:
         # ✅ DEBUG: Log original columns
        app.logger.info(f"[{import_batch_id}] DEBUG - Original columns: {list(df.columns)}")
        
        # ✅ Apply emergency fix first
        df = emergency_column_fix(df, table_name, import_batch_id)
        
        
        # ✅ MAJOR OPTIMIZATION: Use pre-defined column mappings to avoid database queries
        predefined_mappings = {
            'stg_amz_b2b': [
                'seller_gstin', 'invoice_number', 'invoice_date', 'transaction_type', 'order_id', 
                'shipment_id', 'shipment_date', 'order_date', 'shipment_item_id', 'quantity', 
                'item_description', 'asin', 'hsn_sac', 'sku', 'product_tax_code', 'bill_from_city', 
                'bill_from_state', 'bill_from_country', 'bill_from_postal_code', 'ship_from_city', 
                'ship_from_state', 'ship_from_country', 'ship_from_postal_code', 'ship_to_city', 
                'ship_to_state', 'ship_to_country', 'ship_to_postal_code', 'invoice_amount', 
                'tax_exclusive_gross', 'total_tax_amount', 'cgst_rate', 'sgst_rate', 'utgst_rate', 
                'igst_rate', 'compensatory_cess_rate', 'principal_amount', 'principal_amount_basis', 
                'cgst_tax', 'sgst_tax', 'utgst_tax', 'igst_tax', 'compensatory_cess_tax', 
                'shipping_amount', 'shipping_amount_basis', 'shipping_cgst_tax', 'shipping_sgst_tax', 
                'shipping_utgst_tax', 'shipping_igst_tax', 'shipping_cess_tax', 'gift_wrap_amount', 
                'gift_wrap_amount_basis', 'gift_wrap_cgst_tax', 'gift_wrap_sgst_tax', 
                'gift_wrap_utgst_tax', 'gift_wrap_igst_tax', 'gift_wrap_compensatory_cess_tax', 
                'item_promo_discount', 'item_promo_discount_basis', 'item_promo_tax', 
                'shipping_promo_discount', 'shipping_promo_discount_basis', 'shipping_promo_tax', 
                'gift_wrap_promo_discount', 'gift_wrap_promo_discount_basis', 'gift_wrap_promo_tax', 
                'tcs_cgst_rate', 'tcs_cgst_amount', 'tcs_sgst_rate', 'tcs_sgst_amount', 
                'tcs_utgst_rate', 'tcs_utgst_amount', 'tcs_igst_rate', 'tcs_igst_amount', 
                'warehouse_id', 'fulfillment_channel', 'payment_method_code', 'bill_to_city', 
                'bill_to_state', 'bill_to_country', 'bill_to_postalcode', 'customer_bill_to_gstid', 
                'customer_ship_to_gstid', 'buyer_name', 'credit_note_no', 'credit_note_date', 
                'irn_number', 'irn_filing_status', 'irn_date', 'irn_error_code'
            ],
            'stg_amz_b2c': [
                'seller_gstin', 'invoice_number', 'invoice_date', 'transaction_type', 'order_id', 
                'shipment_id', 'shipment_date', 'order_date', 'shipment_item_id', 'quantity', 
                'item_description', 'asin', 'hsn_sac', 'sku', 'product_tax_code', 'bill_from_city', 
                'bill_from_state', 'bill_from_country', 'bill_from_postal_code', 'ship_from_city', 
                'ship_from_state', 'ship_from_country', 'ship_from_postal_code', 'ship_to_city', 
                'ship_to_state', 'ship_to_country', 'ship_to_postal_code', 'invoice_amount', 
                'tax_exclusive_gross', 'total_tax_amount', 'cgst_rate', 'sgst_rate', 'utgst_rate', 
                'igst_rate', 'compensatory_cess_rate', 'principal_amount', 'principal_amount_basis', 
                'cgst_tax', 'sgst_tax', 'igst_tax', 'utgst_tax', 'compensatory_cess_tax', 
                'shipping_amount', 'shipping_amount_basis', 'shipping_cgst_tax', 'shipping_sgst_tax', 
                'shipping_utgst_tax', 'shipping_igst_tax', 'gift_wrap_amount', 'gift_wrap_amount_basis', 
                'gift_wrap_cgst_tax', 'gift_wrap_sgst_tax', 'gift_wrap_utgst_tax', 'gift_wrap_igst_tax', 
                'gift_wrap_compensatory_cess_tax', 'item_promo_discount', 'item_promo_discount_basis', 
                'item_promo_tax', 'shipping_promo_discount', 'shipping_promo_discount_basis', 
                'shipping_promo_tax', 'gift_wrap_promo_discount', 'gift_wrap_promo_discount_basis', 
                'gift_wrap_promo_tax', 'tcs_cgst_rate', 'tcs_cgst_amount', 'tcs_sgst_rate', 
                'tcs_sgst_amount', 'tcs_utgst_rate', 'tcs_utgst_amount', 'tcs_igst_rate', 
                'tcs_igst_amount', 'warehouse_id', 'fulfillment_channel', 'payment_method_code', 
                'credit_note_no', 'credit_note_date'
            ],
            'stg_amz_payout': [
                'date_time', 'settlement_id', 'type', 'order_id', 'sku', 'description', 'quantity', 
                'marketplace', 'account_type', 'fulfillment', 'order_city', 'order_state', 
                'order_postal', 'product_sales', 'shipping_credits', 'gift_wrap_credits', 
                'promotional_rebates', 'total_sales_tax_liable', 'tcs_cgst', 'tcs_sgst', 'tcs_igst', 
                'tds_section_194_o', 'selling_fees', 'fba_fees', 'other_transaction_fees', 'other', 'total'
            ]
        }
        
        # Use predefined mapping if available (FAST PATH)
        if table_name in predefined_mappings:
            db_columns = predefined_mappings[table_name]
            app.logger.info(f"[{import_batch_id}] Using PREDEFINED columns for {table_name}: {len(db_columns)} columns")
        else:
            # Fallback to database lookup (SLOW PATH)
            app.logger.warning(f"[{import_batch_id}] No predefined mapping for {table_name}, falling back to DB lookup")
            conn = get_db_connection()
            db_columns = get_database_columns_fast(table_name, conn)
            conn.close()
            
            if not db_columns:
                app.logger.warning(f"[{import_batch_id}] No database columns found for {table_name}, using direct mapping")
                return direct_column_mapping(df, table_name, import_batch_id)
        
        # FAST column mapping
                # COMPREHENSIVE column mapping for all Amazon variations
        amazon_column_mapping = {
            # Payout file column variations
            'total_sales_tax_liable_gst_before_adjusting_tcs': 'total_sales_tax_liable',
            'total_sales_tax_liable_gst_before_adjusting_tcs_': 'total_sales_tax_liable', 
            'total_sales_tax_liable_gst_before_adjusting_tcs__': 'total_sales_tax_liable',
            'total_sales_tax_liable_gst_before_adjusting_tcs___': 'total_sales_tax_liable',
            'total_sales_tax_liable_gst_before_adjusting_tcs____': 'total_sales_tax_liable',
            'total_sales_tax_liable': 'total_sales_tax_liable',
            
            # Date column variations
            'date/time': 'date_time',
            'date_time': 'date_time',
            'transaction_date': 'date_time',
            'date': 'date_time',
            
            # Settlement ID variations
            'settlement_id': 'settlement_id',
            'settlementid': 'settlement_id',
            'settlement': 'settlement_id',
            
            # Other common variations
            'order_id': 'order_id',
            'orderid': 'order_id',
            'sku': 'sku',
            'description': 'description',
            'quantity': 'quantity',
            'marketplace': 'marketplace',
            'product_sales': 'product_sales',
            'shipping_credits': 'shipping_credits',
            'gift_wrap_credits': 'gift_wrap_credits',
            'promotional_rebates': 'promotional_rebates',
            'tcs_cgst': 'tcs_cgst',
            'tcs_sgst': 'tcs_sgst', 
            'tcs_igst': 'tcs_igst',
            'tds_section_194_o': 'tds_section_194_o',
            'selling_fees': 'selling_fees',
            'fba_fees': 'fba_fees',
            'other_transaction_fees': 'other_transaction_fees',
            'other': 'other',
            'total': 'total'
        }
        
        # Apply quick column mapping
        df = df.rename(columns=amazon_column_mapping)
        
        # FAST column alignment - only keep columns that exist in both
        aligned_columns = {}
        for db_col in db_columns:
            if db_col in df.columns:
                aligned_columns[db_col] = df[db_col]
        
        # Create aligned dataframe
        aligned_df = pd.DataFrame(aligned_columns)
        
        # Log any missing columns
        missing_columns = set(db_columns) - set(aligned_columns.keys())
        if missing_columns:
            app.logger.warning(f"[{import_batch_id}] Missing columns in CSV: {missing_columns}")
            # Add missing columns as NULL
            for col in missing_columns:
                aligned_df[col] = None
        
        app.logger.info(f"[{import_batch_id}] FAST column alignment: {len(aligned_df.columns)} columns, {len(aligned_df)} rows")
        return aligned_df
        
    except Exception as e:
        app.logger.error(f"[{import_batch_id}] Error in fast column validation: {e}")
        # Emergency fallback - just use the original dataframe
        # Emergency fallback - use emergency fix and return original dataframe
        return emergency_column_fix(df, table_name, import_batch_id)

def direct_column_mapping(df, table_name, import_batch_id):
    """Direct column mapping with EXACT database column names"""
    app.logger.info(f"[{import_batch_id}] Using direct column mapping for {table_name}")
    
    # Define EXACT column mappings that match your database schema
    if table_name == 'stg_amz_b2b':
        column_mapping = {
            "seller_gstin": "seller_gstin",
            "invoice_number": "invoice_number", 
            "invoice_date": "invoice_date",
            "transaction_type": "transaction_type",
            "order_id": "order_id",
            "shipment_id": "shipment_id", 
            "shipment_date": "shipment_date",
            "order_date": "order_date",
            "shipment_item_id": "shipment_item_id",
            "quantity": "quantity",
            "item_description": "item_description",
            "asin": "asin",
            "hsn_sac": "hsn_sac", 
            "sku": "sku",
            "product_tax_code": "product_tax_code",
            "bill_from_city": "bill_from_city",
            "bill_from_state": "bill_from_state",
            "bill_from_country": "bill_from_country", 
            "bill_from_postal_code": "bill_from_postal_code",
            "ship_from_city": "ship_from_city",
            "ship_from_state": "ship_from_state",
            "ship_from_country": "ship_from_country",
            "ship_from_postal_code": "ship_from_postal_code",
            "ship_to_city": "ship_to_city",
            "ship_to_state": "ship_to_state", 
            "ship_to_country": "ship_to_country",
            "ship_to_postal_code": "ship_to_postal_code",
            "invoice_amount": "invoice_amount",
            "tax_exclusive_gross": "tax_exclusive_gross",
            "total_tax_amount": "total_tax_amount",
            "cgst_rate": "cgst_rate",
            "sgst_rate": "sgst_rate", 
            "utgst_rate": "utgst_rate",
            "igst_rate": "igst_rate",
            "compensatory_cess_rate": "compensatory_cess_rate",
            "principal_amount": "principal_amount",
            "principal_amount_basis": "principal_amount_basis",
            "cgst_tax": "cgst_tax",
            "sgst_tax": "sgst_tax",
            "igst_tax": "igst_tax", 
            "utgst_tax": "utgst_tax",
            "compensatory_cess_tax": "compensatory_cess_tax",
            "shipping_amount": "shipping_amount",
            "shipping_amount_basis": "shipping_amount_basis",
            "shipping_cgst_tax": "shipping_cgst_tax",
            "shipping_sgst_tax": "shipping_sgst_tax",
            "shipping_utgst_tax": "shipping_utgst_tax", 
            "shipping_igst_tax": "shipping_igst_tax",
            "shipping_cess_tax": "shipping_cess_tax",  # NOTE: Database has this name
            "gift_wrap_amount": "gift_wrap_amount",
            "gift_wrap_amount_basis": "gift_wrap_amount_basis",
            "gift_wrap_cgst_tax": "gift_wrap_cgst_tax",
            "gift_wrap_sgst_tax": "gift_wrap_sgst_tax",
            "gift_wrap_utgst_tax": "gift_wrap_utgst_tax",
            "gift_wrap_igst_tax": "gift_wrap_igst_tax", 
            "gift_wrap_compensatory_cess_tax": "gift_wrap_compensatory_cess_tax",
            "item_promo_discount": "item_promo_discount",
            "item_promo_discount_basis": "item_promo_discount_basis",
            "item_promo_tax": "item_promo_tax",
            "shipping_promo_discount": "shipping_promo_discount",
            "shipping_promo_discount_basis": "shipping_promo_discount_basis",
            "shipping_promo_tax": "shipping_promo_tax", 
            "gift_wrap_promo_discount": "gift_wrap_promo_discount",
            "gift_wrap_promo_discount_basis": "gift_wrap_promo_discount_basis",
            "gift_wrap_promo_tax": "gift_wrap_promo_tax",
            "tcs_cgst_rate": "tcs_cgst_rate",
            "tcs_cgst_amount": "tcs_cgst_amount",
            "tcs_sgst_rate": "tcs_sgst_rate",
            "tcs_sgst_amount": "tcs_sgst_amount",
            "tcs_utgst_rate": "tcs_utgst_rate", 
            "tcs_utgst_amount": "tcs_utgst_amount",
            "tcs_igst_rate": "tcs_igst_rate",
            "tcs_igst_amount": "tcs_igst_amount",
            "warehouse_id": "warehouse_id",
            "fulfillment_channel": "fulfillment_channel",
            "payment_method_code": "payment_method_code",
            "bill_to_city": "bill_to_city",
            "bill_to_state": "bill_to_state",
            "bill_to_country": "bill_to_country",
            "bill_to_postalcode": "bill_to_postalcode",
            "customer_bill_to_gstid": "customer_bill_to_gstid",
            "customer_ship_to_gstid": "customer_ship_to_gstid",
            "buyer_name": "buyer_name",
            "credit_note_no": "credit_note_no",
            "credit_note_date": "credit_note_date",
            "irn_number": "irn_number",
            "irn_filing_status": "irn_filing_status",
            "irn_date": "irn_date",
            "irn_error_code": "irn_error_code"
        }
    elif table_name == 'stg_amz_b2c':
        column_mapping = {
            "seller_gstin": "seller_gstin",
            "invoice_number": "invoice_number", 
            "invoice_date": "invoice_date",
            "transaction_type": "transaction_type",
            "order_id": "order_id",
            "shipment_id": "shipment_id", 
            "shipment_date": "shipment_date",
            "order_date": "order_date",
            "shipment_item_id": "shipment_item_id",
            "quantity": "quantity",
            "item_description": "item_description",
            "asin": "asin",
            "hsn_sac": "hsn_sac", 
            "sku": "sku",
            "product_tax_code": "product_tax_code",
            "bill_from_city": "bill_from_city",
            "bill_from_state": "bill_from_state",
            "bill_from_country": "bill_from_country", 
            "bill_from_postal_code": "bill_from_postal_code",
            "ship_from_city": "ship_from_city",
            "ship_from_state": "ship_from_state",
            "ship_from_country": "ship_from_country",
            "ship_from_postal_code": "ship_from_postal_code",
            "ship_to_city": "ship_to_city",
            "ship_to_state": "ship_to_state", 
            "ship_to_country": "ship_to_country",
            "ship_to_postal_code": "ship_to_postal_code",
            "invoice_amount": "invoice_amount",
            "tax_exclusive_gross": "tax_exclusive_gross",
            "total_tax_amount": "total_tax_amount",
            "cgst_rate": "cgst_rate",
            "sgst_rate": "sgst_rate", 
            "utgst_rate": "utgst_rate",
            "igst_rate": "igst_rate",
            "compensatory_cess_rate": "compensatory_cess_rate",
            "principal_amount": "principal_amount",
            "principal_amount_basis": "principal_amount_basis",
            "cgst_tax": "cgst_tax",
            "sgst_tax": "sgst_tax",
            "igst_tax": "igst_tax", 
            "utgst_tax": "utgst_tax",
            "compensatory_cess_tax": "compensatory_cess_tax",
            "shipping_amount": "shipping_amount",
            "shipping_amount_basis": "shipping_amount_basis",
            "shipping_cgst_tax": "shipping_cgst_tax",
                        "shipping_sgst_tax": "shipping_sgst_tax",
            "shipping_utgst_tax": "shipping_utgst_tax", 
            "shipping_igst_tax": "shipping_igst_tax",
            "gift_wrap_amount": "gift_wrap_amount",
            "gift_wrap_amount_basis": "gift_wrap_amount_basis",
            "gift_wrap_cgst_tax": "gift_wrap_cgst_tax",
            "gift_wrap_sgst_tax": "gift_wrap_sgst_tax",
            "gift_wrap_utgst_tax": "gift_wrap_utgst_tax",
            "gift_wrap_igst_tax": "gift_wrap_igst_tax", 
            "gift_wrap_compensatory_cess_tax": "gift_wrap_compensatory_cess_tax",
            "item_promo_discount": "item_promo_discount",
            "item_promo_discount_basis": "item_promo_discount_basis",
            "item_promo_tax": "item_promo_tax",
            "shipping_promo_discount": "shipping_promo_discount",
            "shipping_promo_discount_basis": "shipping_promo_discount_basis",
            "shipping_promo_tax": "shipping_promo_tax", 
            "gift_wrap_promo_discount": "gift_wrap_promo_discount",
            "gift_wrap_promo_discount_basis": "gift_wrap_promo_discount_basis",
            "gift_wrap_promo_tax": "gift_wrap_promo_tax",
            "tcs_cgst_rate": "tcs_cgst_rate",
            "tcs_cgst_amount": "tcs_cgst_amount",
            "tcs_sgst_rate": "tcs_sgst_rate",
            "tcs_sgst_amount": "tcs_sgst_amount",
            "tcs_utgst_rate": "tcs_utgst_rate", 
            "tcs_utgst_amount": "tcs_utgst_amount",
            "tcs_igst_rate": "tcs_igst_rate",
            "tcs_igst_amount": "tcs_igst_amount",
            "warehouse_id": "warehouse_id",
            "fulfillment_channel": "fulfillment_channel",
            "payment_method_code": "payment_method_code",
            "credit_note_no": "credit_note_no",
            "credit_note_date": "credit_note_date"
        }
    else:
        # Payout mapping (your existing payout mapping)
        column_mapping = {
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
            # ✅ ADD THESE SPECIAL MAPPINGS FOR TOTAL SALES TAX LIABLE
            "total_sales_tax_liable_gst_before_adjusting_tcs": "total_sales_tax_liable",
            "total_sales_tax_liable_gst_before_adjusting_tcs_": "total_sales_tax_liable",
            "total_sales_tax_liable_gst_before_adjusting_tcs__": "total_sales_tax_liable",
            "total_sales_tax_liable": "total_sales_tax_liable",
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
    
    # Apply mapping and keep only mapped columns
    aligned_df = pd.DataFrame()
    
    # First, log what columns we have in the CSV
    app.logger.info(f"[{import_batch_id}] CSV columns found: {list(df.columns)}")
    
    # Map columns that exist in both CSV and database schema
    mapped_count = 0
    for csv_col, db_col in column_mapping.items():
        if csv_col in df.columns:
            aligned_df[db_col] = df[csv_col]
            mapped_count += 1
        # Don't add columns that don't exist in CSV - let them be NULL in database
    
    app.logger.info(f"[{import_batch_id}] Direct mapping result: {list(aligned_df.columns)}")
    app.logger.info(f"[{import_batch_id}] Successfully mapped {mapped_count} columns to database schema")
    
    return aligned_df

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