from flask import Flask, render_template, request, redirect, url_for, session, flash
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
import logging

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


# ... rest of your existing app configuration
app = Flask(__name__)
app.secret_key = "secret_key_123"

# ---------- APPLICATION CONFIG ----------
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB max file size
app.config['UPLOAD_EXTENSIONS'] = ['.csv', '.xlsx', '.xls']
app.config['UPLOAD_FOLDER'] = 'uploads'

# ---------- REQUEST DEDUPLICATION CONFIG ----------
class RequestDeduplication:
    def __init__(self):
        self.active_requests = {}
        self.cleanup_interval = 300  # 5 minutes
        self.request_timeout = 600   # 10 minutes
    
    def generate_request_fingerprint(self, request):
        """Generate unique fingerprint for each upload request"""
        fingerprint_data = {
            'user': session.get('user', 'anonymous'),
            'platform': request.form.get('platform', ''),
            'files': []
        }
        
        # Add file fingerprints
        for file_field in ['b2b_file', 'b2c_file', 'payout_file']:
            file = request.files.get(file_field)
            if file and file.filename:
                file_info = {
                    'field': file_field,
                    'filename': file.filename,
                    'size': len(file.read()),
                    'content_type': file.content_type
                }
                file.seek(0)  # Reset file pointer
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

# Initialize request deduplication
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
#---------------------------------
# Add to your existing imports in app.py

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
    """Check if the file extension is allowed:cite[3]"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in [ext.replace('.', '') for ext in app.config['UPLOAD_EXTENSIONS']]

def secure_file_upload(file):
    """Secure file upload handling:cite[3]:cite[8]"""
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
        print(f"🔄 Columns already normalized for {table_name}, skipping...")
        return df
    
    print(f"🔄 Normalizing columns for {table_name}")
    print(f"📋 Original columns: {list(df.columns)}")
    
    # Single-pass column cleaning
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r'[^a-z0-9]+', '_', regex=True)
        .str.strip('_')
    )
    
    print(f"🔧 After basic cleaning: {list(df.columns)}")
    
    # Special handling for payout files
    if table_name == 'stg_amz_payout':
        df = apply_payout_column_mapping(df)
    else:
        # Apply optimized fuzzy matching for other tables
        df = optimized_fuzzy_match(df, table_name)
    
    # Mark as processed
    df._normalized = True
    df._cache_key = cache_key
    
    print(f"✅ Final columns for {table_name}: {list(df.columns)}")
    return df

def apply_payout_column_mapping(df):
    """
    Apply explicit column mapping for payout files to handle Amazon's column name changes
    """
    print("🎯 Applying payout column mapping...")
    
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
            print(f"📭 Added missing payout column: {expected_col}")
    
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
    
    print(f"💰 Batch processing {len(numeric_cols)} numeric columns")
    
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
    print(f"\n📂 Processing B2B/B2C file: {filepath}")
    
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            if filepath.endswith('.csv'):
                # Use pandas with thousands parameter to handle comma separators during read:cite[2]
                df = pd.read_csv(filepath, encoding=encoding, thousands=',', keep_default_na=True, na_values=['', 'NULL', 'null', 'N/A', 'n/a'])
            else:
                df = pd.read_excel(filepath, keep_default_na=True, na_values=['', 'NULL', 'null', 'N/A', 'n/a'])
            print(f"✅ Successfully read with encoding: {encoding}")
            return df
        except (UnicodeDecodeError, Exception) as e:
            print(f"⚠️ Failed with encoding {encoding}: {e}")
            continue
    
    # Fallback
    try:
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='utf-8', errors='replace', thousands=',')
        else:
            df = pd.read_excel(filepath)
        print("✅ Used UTF-8 with error replacement")
        return df
    except Exception as e:
        raise Exception(f"❌ Could not read file: {str(e)}")

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

    print(f"\n📂 Processing payout file: {filepath}")

    # Step 1: Read raw file text with multiple encodings
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    raw_text = None
    used_encoding = None
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc, errors='replace') as f:
                raw_text = f.read()
            used_encoding = enc
            print(f"✅ Successfully read file with encoding: {enc}")
            break
        except Exception:
            continue
    if raw_text is None:
        raise Exception("❌ Could not read file in any encoding")

    # Step 2: Detect header line (line containing required payout fields)
    header_index = None
    for i, line in enumerate(raw_text.splitlines()):
        line_lower = line.lower()
        if all(k in line_lower for k in ['date/time','settlement id','type','order id']):
            header_index = i
            print(f"📋 Header found at line {i+1}")
            break
    if header_index is None:
        raise Exception("❌ Could not find header line in file")

    # Step 3: Auto-detect delimiter from header line
    first_line = raw_text.splitlines()[header_index]
    possible_delims = [',','\t',';','|']
    delim_counts = {d:first_line.count(d) for d in possible_delims}
    best_delim = max(delim_counts, key=delim_counts.get)
    print(f"🔍 Detected delimiter: '{best_delim}' (counts: {delim_counts})")

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
        print("⚠️ Single-column file detected. Splitting manually...")
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

    print(f"📊 Columns loaded: {len(df.columns)}, Rows: {len(df)}")
    print(df.head(5))
    return df


# ---------- BATCH PROCESSING FUNCTIONS ----------

def batch_process_date_columns(df, table_name):
    """Process date columns in batch"""
    date_columns = ['invoice_date', 'shipment_date', 'order_date', 'credit_note_date', 'irn_date']
    existing_dates = [col for col in date_columns if col in df.columns]
    
    if not existing_dates:
        return df
    
    print(f"📅 Batch processing {len(existing_dates)} date columns")
    
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
    print(f"🔧 Optimizing for table: {table_name}")
    
    # Check if already processed
    if hasattr(df, '_optimized') and getattr(df, '_optimized_for') == table_name:
        print("✅ DataFrame already optimized, skipping...")
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
    
    print(f"✅ Optimized: {original_shape} → {df.shape}")
    validate_dataframe(df, table_name)
    
    return df

# ---------- OPTIMIZED VALIDATION ----------

def validate_dataframe(df, table_name):
    """Optimized data validation"""
    print(f"\n🔍 VALIDATION REPORT for {table_name}:")
    print(f"📊 Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # Null value summary
    null_summary = []
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            null_summary.append(f"   {col}: {null_count} nulls ({null_count/len(df)*100:.1f}%)")
    
    if null_summary:
        print("❌ NULL VALUES:")
        print('\n'.join(null_summary[:10]))  # Show first 10 only
    
    # Date column status
    date_cols = ['invoice_date', 'shipment_date', 'order_date', 'credit_note_date', 'irn_date']
    existing_dates = [col for col in date_cols if col in df.columns]
    
    if existing_dates:
        print("\n📅 DATE STATUS:")
        for col in existing_dates:
            null_count = df[col].isnull().sum()
            unique_dates = df[col].nunique()
            print(f"   {col}: {null_count} nulls, {unique_dates} unique dates")

    # Numeric column validation
    numeric_patterns = ['amount', 'price', 'tax', 'quantity', 'rate', 'total', 'sales', 'credits', 'fees']
    numeric_cols = [col for col in df.columns if any(pattern in col for pattern in numeric_patterns)]
    
    if numeric_cols:
        print("\n💰 NUMERIC COLUMNS STATUS:")
        for col in numeric_cols[:5]:  # Show first 5 numeric columns
            non_numeric = df[col].apply(lambda x: not isinstance(x, (int, float))).sum()
            if non_numeric > 0:
                print(f"   {col}: {non_numeric} non-numeric values")

# ---------- ENHANCED ERROR HANDLING ----------

@app.errorhandler(413)
def too_large(e):
    """Handle file too large errors:cite[3]"""
    flash("File too large. Maximum size is 100MB.", "danger")
    return redirect(url_for('upload'))

@app.errorhandler(500)
def internal_error(e):
    """Handle internal server errors:cite[1]"""
    flash("An internal error occurred. Please try again.", "danger")
    return redirect(url_for('upload'))

# ---------- OPTIMIZED ROUTES ----------

@app.route('/')
def home():
    """Home page route"""
    return render_template('welcome.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username == 'admin' and password == 'admin123':
            session['user'] = username
            flash("Login successful!", "success")
            return redirect(url_for('upload'))
        else:
            flash("Invalid credentials", "danger")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('user', None)
    flash("Logged out successfully", "info")
    return redirect(url_for('home'))

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if 'user' not in session:
        flash("Please login first", "warning")
        return redirect(url_for('login'))

    if request.method == 'POST':
        # 🔒 Generate request fingerprint for deduplication
        request_fingerprint = request_deduplicator.generate_request_fingerprint(request)
        
        # 🔒 Check for duplicate request
        if request_deduplicator.is_duplicate_request(request_fingerprint):
            flash("This upload request is already being processed. Please wait...", "warning")
            app.logger.warning(f"[DUPLICATE_BLOCKED] Request blocked: {request_fingerprint}")
            return redirect(url_for('upload'))
        
        try:
            platform = request.form['platform']
            b2b_file = request.files.get('b2b_file')
            b2c_file = request.files.get('b2c_file')
            payout_file = request.files.get('payout_file')
            
            # Track what files we're processing
            processing_files = {
                'b2b': bool(b2b_file and b2b_file.filename),
                'b2c': bool(b2c_file and b2c_file.filename), 
                'payout': bool(payout_file and payout_file.filename)
            }
            
            results = []
            processed_files = []
            
            # Process B2B file
            if processing_files['b2b']:
                app.logger.info(f"[REQUEST:{request_fingerprint}] Processing B2B file")
                file_results = process_single_file(b2b_file, platform, 'b2b', request_fingerprint)
                results.extend(file_results)
                processed_files.append('sales')
            
            # Process B2C file  
            if processing_files['b2c']:
                app.logger.info(f"[REQUEST:{request_fingerprint}] Processing B2C file")
                file_results = process_single_file(b2c_file, platform, 'b2c', request_fingerprint)
                results.extend(file_results)
                processed_files.append('sales')
            
            # Process Payout file
            if processing_files['payout']:
                app.logger.info(f"[REQUEST:{request_fingerprint}] Processing Payout file")
                file_results = process_single_file(payout_file, platform, 'payout', request_fingerprint)
                results.extend(file_results)
                processed_files.append('payout')
            
            # ✅ CALL RECONCILIATION IF BOTH SALES AND PAYOUT PROCESSED
            if 'sales' in processed_files and 'payout' in processed_files:
                app.logger.info(f"[{request_fingerprint}] BOTH SALES AND PAYOUT COMPLETED - Starting reconciliation")
                try:
                    with psycopg2.connect(**DB_CONFIG) as conn:
                        with conn.cursor() as cur:
                            cur.execute("CALL spigen.process_reconciliation_opt()")
                            conn.commit()
                            results.append("SUCCESS - Reconciliation completed")
                            app.logger.info(f"[{request_fingerprint}] SUCCESS - Reconciliation procedure completed")
                except Exception as recon_error:
                    error_msg = f"ERROR - Reconciliation failed: {str(recon_error)}"
                    results.append(error_msg)
                    app.logger.error(f"[{request_fingerprint}] {error_msg}")

            # Count successes and show results
            total_success = sum(1 for result in results if result.startswith('SUCCESS'))
            
            if results:
                if total_success > 0:
                    flash(" | ".join(results), "success")
                    app.logger.info(f"[SUCCESS:{request_fingerprint}] Upload completed: {total_success} files processed")
                else:
                    flash(" | ".join(results), "warning")
                    app.logger.warning(f"[WARNING:{request_fingerprint}] Upload completed with warnings")
            else:
                flash("No files were processed", "info")
                app.logger.info(f"[INFO:{request_fingerprint}] No files processed")

            # 🔒 Mark request as completed successfully
            request_deduplicator.complete_request(request_fingerprint)
            app.logger.info(f"[COMPLETED:{request_fingerprint}] Request processing completed successfully")

        except Exception as e:
            # 🔒 Ensure request is marked as completed even on error
            request_deduplicator.complete_request(request_fingerprint)
            app.logger.error(f"[ERROR:{request_fingerprint}] Request failed: {str(e)}")
            flash(f"An error occurred during upload: {str(e)}", "danger")
        
        return redirect(url_for('upload'))

    return render_template('upload.html')

def process_single_file(file, platform, table_type, request_fingerprint=None):
    """Process a single file and return results list with enhanced request tracking"""
    import_batch_id = str(uuid.uuid4())
    table_name = f"stg_{platform.lower()}_{table_type.lower()}"
    results = []
    
    # Enhanced logging with request fingerprint
    request_info = f"Request:{request_fingerprint}" if request_fingerprint else "UnknownRequest"
    
    app.logger.info(f"[{request_info}] LOCK - Acquiring lock for {table_name}")
    
    # 🔒 Acquire lock for this table type
    if not lock_manager.acquire_lock(table_name, import_batch_id):
        result_msg = f"[SKIP] {file.filename}: Another {table_type.upper()} import is running"
        results.append(result_msg)
        app.logger.warning(f"[{request_info}] {result_msg}")
        return results
    
    try:
        app.logger.info(f"[{request_info}] [START] Processing {file.filename} as {table_type}")
        log_to_db(import_batch_id, 'UPLOAD', 'start', 'started', 0, f"Processing {file.filename} as {table_type} - {request_info}")

        # Secure file upload
        filename = secure_file_upload(file)
        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        file.save(temp_path)
        
        log_to_db(import_batch_id, 'FILE_UPLOAD', 'save', 'success', 0, f"File uploaded: {filename} - {request_info}")

        # Process file based on table type
        log_to_db(import_batch_id, 'FILE_PROCESSING', 'start', 'started', 0, f"Starting {table_type} file processing - {request_info}")
        
        if table_type.lower() == 'payout':
            df = smart_payout_processor(temp_path)
        else:
            df = process_b2b_b2c_file(temp_path)
        
        log_to_db(import_batch_id, 'FILE_PROCESSING', 'complete', 'success', len(df), f"{table_type} file processed successfully - {request_info}")

        # Optimize dataframe
        df = optimize_dataframe(df, table_name)

        if df.empty:
            result_msg = f"[WARNING] {file.filename}: No data found after processing"
            results.append(result_msg)
            log_to_db(import_batch_id, 'VALIDATION', 'check', 'error', 0, f"No data found after processing - {request_info}")
            app.logger.warning(f"[{request_info}] {result_msg}")
            return results

        # Database operations
        log_to_db(import_batch_id, 'DATABASE', 'truncate', 'started', 0, f"Starting {table_type} database operations - {request_info}")
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Truncate table before copy (safety)
                cur.execute(f"TRUNCATE TABLE spigen.{table_name}")
                log_to_db(import_batch_id, 'DATABASE', 'truncate', 'success', 0, f"Table {table_name} truncated - {request_info}")
                
                # Copy data to staging
                log_to_db(import_batch_id, 'COPY', 'insert_raw_data', 'started', 0, f"Starting {table_type} data copy - {request_info}")
                output = io.StringIO()
                df.to_csv(output, index=False, header=False, na_rep='NULL')
                output.seek(0)
                columns_str = ", ".join(df.columns)
                copy_sql = f"COPY spigen.{table_name} ({columns_str}) FROM STDIN WITH CSV NULL 'NULL'"
                cur.copy_expert(copy_sql, output)
                conn.commit()
                log_to_db(import_batch_id, 'COPY', 'insert_raw_data', 'success', len(df), f"Copied {len(df)} {table_type} records - {request_info}")

                # Execute stored procedures WITH BATCH ID
                if table_type.lower() in ['b2b', 'b2c']:
                    log_to_db(import_batch_id, 'PROCEDURE', 'sales_processing', 'started', 0, f"Starting {table_type} sales processing - {request_info}")
                    
                    # Check if procedure accepts batch_id parameter
                    try:
                        # Try calling with batch_id parameter
                        cur.execute("CALL spigen.process_amazon_sales(%s)", (import_batch_id,))
                        app.logger.info(f"[{request_info}] Called sales procedure with batch_id: {import_batch_id}")
                    except psycopg2.Error as e:
                        # Fallback to original call if procedure doesn't accept parameter
                        app.logger.warning(f"[{request_info}] Procedure doesn't accept batch_id, using original call: {str(e)}")
                        cur.execute("CALL spigen.process_amazon_sales()")
                    
                    conn.commit()
                    log_to_db(import_batch_id, 'PROCEDURE', 'sales_processing', 'success', 0, f"{table_type} sales data processed - {request_info}")

                    # TRUNCATE staging after ETL!
                    cur.execute("TRUNCATE TABLE spigen.stg_amz_b2b")
                    app.logger.info(f"[{request_info}] TRUNCATED staging table: stg_amz_b2b")
                    cur.execute("TRUNCATE TABLE spigen.stg_amz_b2c")
                    app.logger.info(f"[{request_info}] TRUNCATED staging table: stg_amz_b2c")
                    conn.commit()

                if table_type.lower() == 'payout':
                    log_to_db(import_batch_id, 'PROCEDURE', 'payout_processing', 'started', 0, f"Starting {table_type} payout processing - {request_info}")
                    
                    # Check if procedure accepts batch_id parameter
                    try:
                        # Try calling with batch_id parameter
                        cur.execute("CALL spigen.process_amazon_payout(%s)", (import_batch_id,))
                        app.logger.info(f"[{request_info}] Called payout procedure with batch_id: {import_batch_id}")
                    except psycopg2.Error as e:
                        # Fallback to original call if procedure doesn't accept parameter
                        app.logger.warning(f"[{request_info}] Procedure doesn't accept batch_id, using original call: {str(e)}")
                        cur.execute("CALL spigen.process_amazon_payout()")
                    
                    conn.commit()
                    log_to_db(import_batch_id, 'PROCEDURE', 'payout_processing', 'success', 0, f"{table_type} payout data processed - {request_info}")

                    # TRUNCATE staging after ETL!
                    cur.execute("TRUNCATE TABLE spigen.stg_amz_payout")
                    app.logger.info(f"[{request_info}] TRUNCATED staging table: stg_amz_payout")
                    conn.commit()

        # Success
        result_msg = f"SUCCESS - {table_type.upper()}: {len(df)} records (Batch: {import_batch_id[:8]})"
        results.append(result_msg)
        app.logger.info(f"[{request_info}] {result_msg}")
        log_to_db(import_batch_id, 'UPLOAD', 'complete', 'success', len(df), f"{table_type} import completed - {request_info}")

    except Exception as e:
        error_msg = f"ERROR - {table_type.upper()}: {str(e)}"
        results.append(error_msg)
        app.logger.error(f"[{request_info}] {error_msg}")
        log_to_db(import_batch_id, 'UPLOAD', 'process', 'error', 0, f"{table_type} import failed: {str(e)} - {request_info}", str(e))
        
    finally:
        # 🔒 Always release lock
        lock_manager.release_lock(table_name, import_batch_id)
        app.logger.info(f"[{request_info}] UNLOCK - Released lock for {table_name}")
        
        # Cleanup temporary file
        if 'temp_path' in locals() and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                app.logger.info(f"[{request_info}] CLEANUP - Cleaned up temporary file: {temp_path}")
            except Exception as e:
                app.logger.warning(f"[{request_info}] Failed to cleanup temp file {temp_path}: {str(e)}")
    
    return results

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
    
    return render_template('logs.html', file_logs=log_entries, db_logs=db_logs, active_requests=active_requests)

@app.route('/admin/requests')
def view_active_requests():
    """Admin endpoint to view active requests (optional)"""
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

# Add cleanup on app shutdown
import atexit

@atexit.register
def cleanup_on_shutdown():
    """Clean up all active requests on application shutdown"""
    app.logger.info("🛑 Application shutting down - cleaning up active requests")
    request_deduplicator.active_requests.clear()
    app.logger.info("✅ All active requests cleared")

if __name__ == '__main__':
    # Create uploads directory if it doesn't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Clean up any expired requests on startup
    request_deduplicator.cleanup_expired_requests()
    
    app.logger.info("🚀 Flask application started with request deduplication")
    app.run(debug=True)