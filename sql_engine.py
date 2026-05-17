import re
import os
import json
import csv
from collections import defaultdict
from datetime import datetime

class DataCache:
    """Efficient data caching to avoid multiple file reads"""
    def __init__(self):
        self.headers = None
        self.sample_rows = None
        self.all_rows = None
        self.file_info = None
        self.is_loaded = False
        self.is_large_file = False
        self.chunk_generator = None
        self.file_type = None
        
    def clear(self):
        """Clear cached data"""
        self.headers = None
        self.sample_rows = None
        self.all_rows = None
        self.file_info = None
        self.is_loaded = False
        self.is_large_file = False
        self.chunk_generator = None
        self.file_type = None
        
    def get_file_type(self, file_path):
        """Determine file type based on extension"""
        extension = os.path.splitext(file_path)[1].lower()
        if extension == '.json':
            return 'json'
        else:
            return 'csv'
        
    def load_file(self, file_path, delimiter, sample_percentage=15, large_file_threshold=50000):
        """Load file with smart caching strategy"""
        self.clear()
        
        # Determine file type
        self.file_type = self.get_file_type(file_path)
        
        if self.file_type == 'json':
            self._load_json_file(file_path, sample_percentage, large_file_threshold)
        else:
            self._load_csv_file(file_path, delimiter, sample_percentage, large_file_threshold)
            
        self.file_info = {
            'total_rows': len(self.all_rows) if self.all_rows else getattr(self, 'estimated_rows', 0),
            'delimiter': delimiter if self.file_type == 'csv' else 'N/A (JSON)',
            'file_path': file_path,
            'is_large_file': self.is_large_file,
            'estimated_rows': getattr(self, 'estimated_rows', len(self.all_rows) if self.all_rows else 0),
            'file_type': self.file_type
        }
        self.is_loaded = True
        
    def _load_json_file(self, file_path, sample_percentage, large_file_threshold):
        """Load and process JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # First, try to get file size estimate
                f.seek(0, 2)  # Go to end
                file_size = f.tell()
                f.seek(0)  # Go back to start
                
                # Load JSON data
                json_data = json.load(f)
                
            # Process JSON into tabular format
            processed_data = self._process_json_data(json_data)
            
            if not processed_data:
                raise ValueError("No valid tabular data found in JSON file")
                
            self.headers = processed_data['headers']
            self.all_rows = processed_data['rows']
            self.estimated_rows = len(self.all_rows)
            
            # Determine if it's a large file
            self.is_large_file = len(self.all_rows) > large_file_threshold
            
            # Create sample for type inference
            sample_size = max(100, int(len(self.all_rows) * sample_percentage / 100))
            self.sample_rows = self.all_rows[:sample_size]
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON file: {e}")
        except Exception as e:
            raise ValueError(f"Error processing JSON file: {e}")
    
    def _process_json_data(self, json_data):
        """Convert JSON data to tabular format"""
        if isinstance(json_data, list):
            # Array of objects - most common case
            if not json_data:
                return {'headers': [], 'rows': []}
                
            # If it's a list of primitives, convert to single column
            if not isinstance(json_data[0], dict):
                return {
                    'headers': ['value'],
                    'rows': [[str(item)] for item in json_data]
                }
                
            # Process array of objects - preserve order
            headers = []
            processed_rows = []
            
            # First pass: collect all possible headers in order of appearance
            for item in json_data:
                if isinstance(item, dict):
                    flattened = self._flatten_object(item)
                    # Add new headers in the order they appear
                    for key in flattened.keys():
                        if key not in headers:
                            headers.append(key)
            
            # Second pass: create rows with consistent column structure
            for item in json_data:
                if isinstance(item, dict):
                    flattened = self._flatten_object(item)
                    row = [str(flattened.get(header, '')) for header in headers]
                    processed_rows.append(row)
                    
        elif isinstance(json_data, dict):
            # Single object - convert to single row, preserve key order
            flattened = self._flatten_object(json_data)
            headers = list(flattened.keys())  # Maintain order from the object
            processed_rows = [[str(flattened.get(header, '')) for header in headers]]
            
        else:
            # Single primitive value
            headers = ['value']
            processed_rows = [[str(json_data)]]
            
        return {
            'headers': headers,
            'rows': processed_rows
        }
    
    def _flatten_object(self, obj, parent_key='', sep='.'):
        """Flatten nested JSON objects using dot notation, preserving key order"""
        items = []
        
        if isinstance(obj, dict):
            # Process keys in order they appear in the dictionary
            for key, value in obj.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                
                if isinstance(value, dict):
                    items.extend(self._flatten_object(value, new_key, sep).items())
                elif isinstance(value, list):
                    # Handle arrays - convert to comma-separated string for simplicity
                    if value and isinstance(value[0], dict):
                        # Array of objects - flatten each and number them
                        for i, item in enumerate(value):
                            items.extend(self._flatten_object(item, f"{new_key}[{i}]", sep).items())
                    else:
                        # Array of primitives - join as string
                        items.append((new_key, ', '.join(str(v) for v in value)))
                else:
                    items.append((new_key, value))
        else:
            # Handle case where obj is not a dict (shouldn't happen in normal flattening)
            items.append((parent_key or 'value', obj))
            
        return dict(items)
        
    def _load_csv_file(self, file_path, delimiter, sample_percentage, large_file_threshold):
        """Load CSV file (original logic)"""
        # Get file size estimate
        with open(file_path, 'r', newline='') as f:
            # Read first few lines to estimate
            sample_lines = []
            for i, line in enumerate(f):
                if i >= 100:
                    break
                sample_lines.append(line)
            
            f.seek(0, 2)  # Go to end
            file_size = f.tell()
            f.seek(0)  # Go back to start
            
            # Estimate total rows
            avg_line_size = file_size / len(sample_lines) if sample_lines else 100
            estimated_rows = int(file_size / avg_line_size)
            
        self.is_large_file = estimated_rows > large_file_threshold
        self.estimated_rows = estimated_rows
        
        if self.is_large_file:
            self._load_large_csv_file(file_path, delimiter, sample_percentage)
        else:
            self._load_small_csv_file(file_path, delimiter, sample_percentage)
            
    def _load_small_csv_file(self, file_path, delimiter, sample_percentage):
        """Load entire CSV file for small datasets"""
        with open(file_path, 'r', newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            self.headers = next(reader)
            self.all_rows = list(reader)
            
        # Create sample for type inference
        sample_size = max(100, int(len(self.all_rows) * sample_percentage / 100))
        self.sample_rows = self.all_rows[:sample_size]
        
    def _load_large_csv_file(self, file_path, delimiter, sample_percentage, chunk_size=10000):
        """Load only sample for large CSV files"""
        sample_target = max(1000, int(chunk_size * sample_percentage / 100))
        
        with open(file_path, 'r', newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            self.headers = next(reader)
            
            # Load sample for type inference and preview
            self.sample_rows = []
            for i, row in enumerate(reader):
                if i >= sample_target:
                    break
                self.sample_rows.append(row)
                
        # Don't load all_rows for large files - use generator instead
        self.all_rows = None
        
    def get_chunk_generator(self, chunk_size=5000):
        """Get generator for chunked processing of large files"""
        if self.file_type == 'json':
            # For JSON, chunk the loaded data
            if self.all_rows:
                for i in range(0, len(self.all_rows), chunk_size):
                    yield self.all_rows[i:i + chunk_size]
        elif not self.is_large_file and self.all_rows:
            # For small CSV files, chunk the loaded data
            for i in range(0, len(self.all_rows), chunk_size):
                yield self.all_rows[i:i + chunk_size]
        else:
            # For large CSV files, read chunks from file
            with open(self.file_info['file_path'], 'r', newline='') as f:
                reader = csv.reader(f, delimiter=self.file_info['delimiter'])
                next(reader)  # Skip headers
                
                chunk = []
                for row in reader:
                    chunk.append(row)
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                if chunk:  # Yield remaining rows
                    yield chunk

class OptimizedTypeInferrer:
    """Optimized type inference with regex patterns and statistical sampling"""
    def __init__(self):
        # Compile regex patterns once for reuse
        self.int_pattern = re.compile(r'^-?\d+$')
        self.float_pattern = re.compile(r'^-?\d*\.\d+$')
        self.date_patterns = [
            re.compile(r'^\d{4}-\d{2}-\d{2}$'),  # YYYY-MM-DD
            re.compile(r'^\d{2}/\d{2}/\d{4}$'),  # MM/DD/YYYY
            re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'),  # datetime
            re.compile(r'^\d{2}-\d{2}-\d{4}$'),  # DD-MM-YYYY
        ]
    
    def infer_column_types(self, sample_rows, headers, max_sample=1000):
        """Efficiently infer types using statistical sampling"""
        if not sample_rows:
            return ["NVARCHAR(255)"] * len(headers)
        
        # Limit sample size for performance
        sample_data = sample_rows[:max_sample]
        column_count = len(headers)
        
        # Initialize type counters
        type_votes = [defaultdict(int) for _ in range(column_count)]
        max_lengths = [0] * column_count
        
        for row in sample_data:
            # Pad row if necessary
            padded_row = row + [''] * (column_count - len(row))
            
            for col_idx, value in enumerate(padded_row[:column_count]):
                if col_idx >= len(type_votes):
                    continue
                    
                value = str(value).strip()
                max_lengths[col_idx] = max(max_lengths[col_idx], len(value))
                
                if not value:  # Empty value
                    continue
                
                # Quick type checks using compiled patterns
                if value.lower() in ('0', '1', 'true', 'false'):
                    type_votes[col_idx]['BIT'] += 1
                elif self.int_pattern.match(value):
                    type_votes[col_idx]['INT'] += 1
                elif self.float_pattern.match(value):
                    type_votes[col_idx]['FLOAT'] += 1
                elif any(pattern.match(value) for pattern in self.date_patterns):
                    type_votes[col_idx]['DATETIME'] += 1
                else:
                    type_votes[col_idx]['VARCHAR'] += 1
        
        # Determine final types based on votes
        inferred_types = []
        for col_idx in range(column_count):
            votes = type_votes[col_idx]
            max_len = max_lengths[col_idx]
            
            if not votes:
                inferred_types.append("NVARCHAR(255)")
                continue
            
            # Get most common type
            best_type = max(votes.keys(), key=lambda k: votes[k])
            
            if best_type == 'VARCHAR':
                if max_len <= 10:
                    inferred_types.append("NVARCHAR(10)")
                elif max_len <= 50:
                    inferred_types.append("NVARCHAR(50)")
                elif max_len <= 255:
                    inferred_types.append("NVARCHAR(255)")
                elif max_len <= 4000:
                    inferred_types.append(f"NVARCHAR({max_len})")
                else:
                    inferred_types.append("NVARCHAR(MAX)")
            else:
                inferred_types.append(best_type)
        
        return inferred_types

class SQLGenerator:
    """Core logic for generating SQL statements and formatting names"""
    
    @staticmethod
    def is_quoted_type(sql_type: str) -> bool:
        sql_type_upper = sql_type.strip().upper()
        unquoted_keywords = ['INT', 'FLOAT', 'BIT', 'DECIMAL', 'NUMERIC', 'REAL', 'SMALLINT', 'TINYINT', 'BIGINT']
        for keyword in unquoted_keywords:
            if sql_type_upper.startswith(keyword):
                return False
        return True

    @staticmethod
    def format_insert_values(row, column_types):
        formatted_values = []
        for val, sql_type in zip(row, column_types):
            sql_type_upper = sql_type.strip().upper()
            if 'INT IDENTITY' in sql_type_upper:
                continue  # Skip values for INT IDENTITY
            elif 'UNIQUEIDENTIFIER' in sql_type_upper and val.strip() == '':
                formatted_values.append('NEWID()')
            elif val == '':
                formatted_values.append('NULL')
            elif SQLGenerator.is_quoted_type(sql_type):
                escaped_val = val.replace("'", "''")
                formatted_values.append(f"'{escaped_val}'")
            else:
                formatted_values.append(val)
        return f"    ({', '.join(formatted_values)})"

    @staticmethod
    def format_column_name(name: str, style: str) -> str:
        parts = re.split(r'[\s_\-]+', name)
        parts = [word for part in parts for word in re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)', part)]
        parts = [p for p in parts if p]
        if style == "snake_case":
            return "_".join(p.lower() for p in parts)
        elif style == "CamelCase":
            return "".join(p.capitalize() for p in parts)
        elif style == "lowercase":
            return "".join(p.lower() for p in parts)
        elif style == "UPPERCASE":
            return "".join(p.upper() for p in parts)
        else:
            return name

    @staticmethod
    def generate_create_table_script(db_name, schema_name, table_name, columns):
        """
        columns: list of dicts with keys 'name', 'type', 'is_pk', 'allows_null'
        """
        full_table = f"[{schema_name}].[{table_name}]"
        create_lines = []

        if db_name:
            create_lines.append(f"USE [{db_name}];")
            create_lines.append("GO")
            create_lines.append("")

        create_lines.append(f"CREATE TABLE {full_table} (")

        pk_columns = []
        for i, col in enumerate(columns):
            null_str = "NULL" if col['allows_null'] else "NOT NULL"
            column_def = f"    [{col['name']}] {col['type']} {null_str}"
            if i < len(columns) - 1 or any(c['is_pk'] for c in columns):
                column_def += ","
            create_lines.append(column_def)
            if col['is_pk']:
                pk_columns.append(col['name'])

        if pk_columns:
            constraint_name = f"PK_{table_name}"
            pk_line = f"    CONSTRAINT {constraint_name} PRIMARY KEY ({', '.join(f'[{c}]' for c in pk_columns)})"
            create_lines.append(pk_line)

        create_lines.append(");")
        return "\n".join(create_lines)

    @staticmethod
    def generate_insert_script(file_path, table_name, schema_name, db_name, col_names, column_types, 
                              data_cache, batch_insert, batch_size, truncate_before_insert, 
                              progress_callback=None, cancel_check=None):
        """
        Generates INSERT statements and writes them to a file.
        Returns the total number of rows processed.
        """
        full_table = f"[{schema_name}].[{table_name}]"
        script_lines = []

        if db_name:
            script_lines.append(f"USE [{db_name}];")
            script_lines.append("GO")
            script_lines.append("")

        if truncate_before_insert:
            script_lines.append(f"TRUNCATE TABLE {full_table};")
            script_lines.append("GO")
            script_lines.append("")

        insert_header = f"INSERT INTO {full_table} ({', '.join(f'[{c}]' for c in col_names)})\nVALUES"
        
        if progress_callback:
            progress_callback("Reading and processing data...")
            
        # Process data in chunks
        all_values = []
        chunk_count = 0
        total_rows_processed = 0
        
        for chunk in data_cache.get_chunk_generator():
            if cancel_check and cancel_check():
                return None
                
            chunk_count += 1
            if progress_callback:
                progress_callback(f"Processing data chunk {chunk_count}...")
            
            for row in chunk:
                # Pad row if necessary
                extra_count = len(column_types) - len(row)
                if extra_count > 0:
                    row = [''] * extra_count + row
                all_values.append(SQLGenerator.format_insert_values(row, column_types))
                total_rows_processed += 1
                
                if total_rows_processed % 5000 == 0 and progress_callback:
                    progress_callback(f"Processed {total_rows_processed:,} rows...")
        
        if cancel_check and cancel_check():
            return None
            
        if progress_callback:
            progress_callback(f"Generating SQL script for {total_rows_processed:,} rows...")
        
        if batch_insert:
            batch_count = 0
            total_batches = (len(all_values) + batch_size - 1) // batch_size
            
            for i in range(0, len(all_values), batch_size):
                if cancel_check and cancel_check():
                    return None
                    
                batch_count += 1
                if progress_callback:
                    progress_callback(f"Creating batch {batch_count} of {total_batches}...")
                
                chunk = all_values[i:i + batch_size]
                script_lines.append(insert_header)
                script_lines.append(",\n".join(chunk) + ";\nGO")
        else:
            script_lines.append(insert_header)
            script_lines.append(",\n".join(all_values) + ";\nGO")

        script = "\n".join(script_lines)
        
        if progress_callback:
            progress_callback("Saving file to disk...")
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(script)
            
        return total_rows_processed

    @staticmethod
    def write_operation_log(log_data, enable_logging, log_directory):
        """Write comprehensive operation log"""
        if not enable_logging:
            return
            
        try:
            # Determine log directory
            log_dir = log_directory.strip()
            if not log_dir:
                # Use same directory as the first script file
                if log_data.get('create_script_path'):
                    log_dir = os.path.dirname(log_data['create_script_path'])
                elif log_data.get('insert_script_path'):
                    log_dir = os.path.dirname(log_data['insert_script_path'])
                else:
                    log_dir = os.getcwd()  # Fallback to current directory
            
            # Create log directory if it doesn't exist
            os.makedirs(log_dir, exist_ok=True)
            
            # Generate log filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_name = log_data.get('table_name', 'unknown')
            log_filename = f"SQLTableBuilder_{table_name}_{timestamp}.log"
            log_path = os.path.join(log_dir, log_filename)
            
            # Write comprehensive log
            with open(log_path, 'w', encoding='utf-8') as log_file:
                log_file.write("="*80 + "\n")
                log_file.write("SQL TABLE BUILDER PRO - OPERATION LOG\n")
                log_file.write("="*80 + "\n\n")
                
                # Timestamp
                log_file.write(f"Operation Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_file.write(f"Log File: {log_filename}\n\n")
                
                # Source File Information
                log_file.write("SOURCE FILE INFORMATION:\n")
                log_file.write("-" * 40 + "\n")
                log_file.write(f"File Path: {log_data.get('source_file_path', 'N/A')}\n")
                log_file.write(f"File Name: {log_data.get('source_file_name', 'N/A')}\n")
                log_file.write(f"File Type: {log_data.get('source_file_type', 'N/A')}\n")
                log_file.write(f"File Size: {log_data.get('source_file_size', 'N/A')}\n")
                log_file.write(f"Total Rows in Source: {log_data.get('source_total_rows', 'N/A'):,}\n")
                if log_data.get('source_delimiter'):
                    log_file.write(f"Delimiter: {log_data.get('source_delimiter', 'N/A')}\n")
                log_file.write("\n")
                
                # Table Configuration
                log_file.write("TABLE CONFIGURATION:\n")
                log_file.write("-" * 40 + "\n")
                log_file.write(f"Database: {log_data.get('database_name', 'N/A')}\n")
                log_file.write(f"Schema: {log_data.get('schema_name', 'N/A')}\n")
                log_file.write(f"Table: {log_data.get('table_name', 'N/A')}\n")
                log_file.write(f"Full Table Name: {log_data.get('full_table_name', 'N/A')}\n")
                log_file.write(f"Column Count: {log_data.get('column_count', 'N/A')}\n")
                if log_data.get('primary_key_columns'):
                    log_file.write(f"Primary Key Columns: {', '.join(log_data['primary_key_columns'])}\n")
                log_file.write("\n")
                
                # Column Details
                if log_data.get('column_details'):
                    log_file.write("COLUMN DETAILS:\n")
                    log_file.write("-" * 40 + "\n")
                    for i, col in enumerate(log_data['column_details'], 1):
                        pk_indicator = " (PK)" if col.get('is_primary_key') else ""
                        null_indicator = " NULL" if col.get('allows_null') else " NOT NULL"
                        log_file.write(f"{i:2d}. {col['name']:<25} {col['type']:<20} {null_indicator}{pk_indicator}\n")
                    log_file.write("\n")
                
                # Scripts Generated
                log_file.write("SCRIPTS GENERATED:\n")
                log_file.write("-" * 40 + "\n")
                
                if log_data.get('create_script_generated'):
                    log_file.write("✓ CREATE TABLE script generated\n")
                    log_file.write(f"  File: {log_data.get('create_script_name', 'N/A')}\n")
                    log_file.write(f"  Path: {log_data.get('create_script_path', 'N/A')}\n")
                    log_file.write(f"  Size: {log_data.get('create_script_size', 'N/A')}\n")
                else:
                    log_file.write("✗ CREATE TABLE script not generated\n")
                
                if log_data.get('insert_script_generated'):
                    log_file.write("✓ INSERT statements generated\n")
                    log_file.write(f"  File: {log_data.get('insert_script_name', 'N/A')}\n")
                    log_file.write(f"  Path: {log_data.get('insert_script_path', 'N/A')}\n")
                    log_file.write(f"  Size: {log_data.get('insert_script_size', 'N/A')}\n")
                    log_file.write(f"  Rows Processed: {log_data.get('insert_rows_processed', 'N/A'):,}\n")
                else:
                    log_file.write("✗ INSERT statements not generated\n")
                
                log_file.write("\n")
                
                # Processing Settings
                log_file.write("PROCESSING SETTINGS:\n")
                log_file.write("-" * 40 + "\n")
                log_file.write(f"Data Type Inference: {'Enabled' if log_data.get('type_inference_enabled') else 'Disabled'}\n")
                log_file.write(f"Column Format: {log_data.get('column_format', 'N/A')}\n")
                if log_data.get('insert_script_generated'):
                    log_file.write(f"Batch Insert: {'Enabled' if log_data.get('batch_insert_enabled') else 'Disabled'}\n")
                    if log_data.get('batch_insert_enabled'):
                        log_file.write(f"Batch Size: {log_data.get('batch_size', 'N/A'):,}\n")
                    log_file.write(f"Truncate Before Insert: {'Enabled' if log_data.get('truncate_enabled') else 'Disabled'}\n")
                log_file.write("\n")
                
                # Data Validation
                log_file.write("DATA VALIDATION:\n")
                log_file.write("-" * 40 + "\n")
                source_rows = log_data.get('source_total_rows', 0)
                processed_rows = log_data.get('insert_rows_processed', 0)
                
                if log_data.get('insert_script_generated'):
                    if source_rows == processed_rows:
                        log_file.write("✓ Row count validation PASSED\n")
                        log_file.write(f"  Source rows: {source_rows:,}\n")
                        log_file.write(f"  Processed rows: {processed_rows:,}\n")
                    else:
                        log_file.write("⚠ Row count validation FAILED\n")
                        log_file.write(f"  Source rows: {source_rows:,}\n")
                        log_file.write(f"  Processed rows: {processed_rows:,}\n")
                        log_file.write(f"  Difference: {abs(source_rows - processed_rows):,}\n")
                else:
                    log_file.write("- Row count validation not applicable (INSERT script not generated)\n")
                
                log_file.write("\n")
                
                # Summary
                log_file.write("OPERATION SUMMARY:\n")
                log_file.write("-" * 40 + "\n")
                scripts_generated = []
                if log_data.get('create_script_generated'):
                    scripts_generated.append("CREATE TABLE")
                if log_data.get('insert_script_generated'):
                    scripts_generated.append("INSERT")
                
                log_file.write(f"Scripts Generated: {', '.join(scripts_generated) if scripts_generated else 'None'}\n")
                log_file.write(f"Total Processing Time: {log_data.get('total_processing_time', 'N/A')}\n")
                log_file.write(f"Operation Status: {'SUCCESS' if log_data.get('operation_successful', True) else 'FAILED'}\n")
                
                if log_data.get('notes'):
                    log_file.write(f"Notes: {log_data['notes']}\n")
                
                log_file.write("\n")
                log_file.write("="*80 + "\n")
                log_file.write("END OF LOG\n")
                log_file.write("="*80 + "\n")
                
            print(f"Operation log written to: {log_path}")
            
        except Exception as e:
            print(f"Error writing operation log: {e}")
