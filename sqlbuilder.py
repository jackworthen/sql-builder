from config_manager import ConfigManager
import re
from datetime import datetime
from collections import defaultdict, Counter
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import json
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog, messagebox
import csv
import sys
import os

def resource_path(filename):
    """ Get absolute path to resource, works for dev and for PyInstaller bundle """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.abspath("."), filename)

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
            
            # For very large JSON files, we might want to clear all_rows and use generator
            if self.is_large_file:
                # Keep all_rows for JSON since we've already loaded it into memory
                # JSON files are typically smaller and already fully loaded
                pass
                
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
            
        # Return as OrderedDict to preserve insertion order, but convert to regular dict
        # since Python 3.7+ regular dicts maintain insertion order
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

class ProgressWindow:
    """Progress dialog for long-running operations"""
    def __init__(self, parent, title="Processing..."):
        self.window = tk.Toplevel(parent)
        self.window.title(title)
        self.window.geometry("400x150")
        self.window.transient(parent)
        self.window.grab_set()
        
        # Center the window
        self.window.update_idletasks()
        x = (self.window.winfo_screenwidth() // 2) - (400 // 2)
        y = (self.window.winfo_screenheight() // 2) - (150 // 2)
        self.window.geometry(f"400x150+{x}+{y}")
        
        self.label = tk.Label(self.window, text="Initializing...", justify='left')
        self.label.pack(pady=20)
        
        self.progress = ttk.Progressbar(self.window, mode='indeterminate')
        self.progress.pack(pady=10, padx=40, fill='x')
        self.progress.start()
        
        # Create style for buttons
        style = ttk.Style()
        style.configure('ProgressCancel.TButton', 
                       background='#FFB6C1',  # Light pink
                       foreground='black',
                       padding=(5, 2),        # Smaller padding
                       relief='flat',
                       borderwidth=1,
                       font=('Arial', 8))
        
        # Also configure the light blue style for completion
        style.configure('LightBlue.TButton',
                       background='#ADD8E6',  # Light blue
                       foreground='black',
                       padding=(5, 2),
                       relief='flat',
                       borderwidth=1,
                       font=('Arial', 8))
        
        self.cancel_button = ttk.Button(self.window, text="Cancel", 
                                       style='ProgressCancel.TButton',
                                       command=self.cancel)
        self.cancel_button.pack(pady=10)
        
        self.cancelled = False
        
    def update_text(self, text):
        self.label.config(text=text)
        self.window.update()
        
    def set_progress(self, value, maximum=100):
        self.progress.config(mode='determinate', maximum=maximum, value=value)
        self.window.update()
        
    def show_completion(self, dual_scripts=False):
        """Show completion state - stop progress bar and change button to Close"""
        self.progress.stop()
        self.progress.pack_forget()  # Hide the progress bar
        self.cancel_button.config(text="Close", 
                                 style='LightBlue.TButton',  # Change to normal button style
                                 command=self.close)
        self.window.title("Operation Complete")
        
        # Increase window height based on content - smaller since we removed file paths
        window_height = 180 if dual_scripts else 150
        self.window.geometry(f"450x{window_height}")
        
        # Re-center the window with new size
        self.window.update_idletasks()
        x = (self.window.winfo_screenwidth() // 2) - (450 // 2)
        y = (self.window.winfo_screenheight() // 2) - (window_height // 2)
        self.window.geometry(f"450x{window_height}+{x}+{y}")
        
    def cancel(self):
        self.cancelled = True
        self.close()
        
    def close(self):
        self.progress.stop()
        self.window.destroy()

class SQLTableBuilder:
    def is_quoted_type(self, sql_type: str) -> bool:
        sql_type_upper = sql_type.strip().upper()
        unquoted_keywords = ['INT', 'FLOAT', 'BIT', 'DECIMAL', 'NUMERIC', 'REAL', 'SMALLINT', 'TINYINT', 'BIGINT']
        for keyword in unquoted_keywords:
            if sql_type_upper.startswith(keyword):
                return False
        return True

    def format_insert_values(self, row, column_types):
        formatted_values = []
        for val, sql_type in zip(row, column_types):
            sql_type_upper = sql_type.strip().upper()
            if 'INT IDENTITY' in sql_type_upper:
                continue  # Skip values for INT IDENTITY
            elif 'UNIQUEIDENTIFIER' in sql_type_upper and val.strip() == '':
                formatted_values.append('NEWID()')
            elif val == '':
                formatted_values.append('NULL')
            elif self.is_quoted_type(sql_type):
                escaped_val = val.replace("'", "''")
                formatted_values.append(f"'{escaped_val}'")
            else:
                formatted_values.append(val)
        return f"    ({', '.join(formatted_values)})"

    def __init__(self, master):
        self.master = master
        self.master.title("SQL Table Builder Pro")
        self.master.geometry("750x500")  
        self.file_path = tk.StringVar()
        self.delimiter = tk.StringVar()
        self.table_name = tk.StringVar()
        self.schema_name = tk.StringVar(value="dbo")
        self.headers = []
        self.original_headers = []  # Store original headers for "Source File" reset
        self.column_entries = []
        self.type_entries = []
        self.pk_vars = []
        self.pk_checkboxes = []
        self.null_vars = []
        self.null_checkboxes = []
        self.database_name = tk.StringVar()
        self.infer_types_var = tk.BooleanVar(value=True)
        self.include_create_script = tk.BooleanVar(value=True)
        self.include_insert_script = tk.BooleanVar(value=True)
        
        # Initialize optimized components
        self.data_cache = DataCache()
        self.type_inferrer = OptimizedTypeInferrer()
        self.executor = ThreadPoolExecutor(max_workers=2)
        
        # Configure button styling
        self.setup_button_styles()
        
        self.config_mgr = ConfigManager()
        cfg = self.config_mgr.config
        self.additional_column_count = 0
        self.max_additional_columns = int(cfg.get("max_additional_columns", 1))
        self.schema_name.set(cfg["default_schema"])
        self.preview_percentage_var = tk.StringVar(value=str(cfg["default_preview_percentage"]))
        self.include_create_script = tk.BooleanVar(value=cfg["default_include_create"])
        self.include_insert_script = tk.BooleanVar(value=cfg["default_include_insert"])
        self.batch_insert_var = tk.BooleanVar(value=cfg.get("default_batch_insert", True))
        self.insert_batch_size = int(cfg.get("insert_batch_size") or 500)
        self.truncate_before_insert = tk.BooleanVar()
        self.infer_types_var = tk.BooleanVar(value=cfg["default_infer_types"])
        self.sample_percentage = cfg["sample_percentage"]
        
        # New table name configuration settings
        self.use_filename_as_table_name = cfg.get("use_filename_as_table_name", True)
        self.custom_table_name = cfg.get("custom_table_name", "")
        
        # NEW: Auto preview data setting
        self.auto_preview_data = cfg.get("auto_preview_data", True)
        
        # NEW: Large file threshold setting
        self.large_file_threshold_mb = cfg.get("large_file_threshold_mb", 1000)
        
        # Initialize large file indicator widget
        self.large_file_indicator = None

        self.apply_config_settings()

        self.sql_data_types = [
            "BIGINT", "BINARY", "BIT", "CHAR(n)", "DATE", "DATETIME", "DATETIME2",
            "DATETIMEOFFSET", "DECIMAL(p,s)", "FLOAT", "IMAGE", "INT", "NCHAR(n)", "NTEXT",
            "NUMERIC(p,s)", "NVARCHAR(MAX)", "NVARCHAR(255)", "NVARCHAR(n)", "REAL", "SMALLINT", "SMALLDATETIME",
            "TEXT", "TIME", "TINYINT", "VARBINARY(MAX)", "VARBINARY(n)", "VARCHAR(MAX)", "VARCHAR(n)"
        ]

        self.build_file_selection_screen()

    def setup_button_styles(self):
        """Configure button styles with compact design and light blue theme"""
        self.style = ttk.Style()
        
        # Configure primary button style - even smaller with black text on light blue
        self.style.configure('Primary.TButton',
                            background='#ADD8E6',  # Light blue
                            foreground='black',
                            padding=(6, 3),        # Further reduced from (8, 4)
                            relief='flat',
                            borderwidth=1,
                            focuscolor='none',
                            font=('Arial', 8))
        
        # Configure hover and pressed effects
        self.style.map('Primary.TButton',
                      background=[('active', '#87CEEB'),   # Slightly darker light blue
                                 ('pressed', '#87CEFA')],  # Sky blue
                      relief=[('pressed', 'flat'),
                             ('!pressed', 'flat')])
        
        # Small button variant for compact spaces
        self.style.configure('Small.TButton',
                            background='#B0E0E6',  # Powder blue
                            foreground='black',
                            padding=(4, 2),        # Further reduced from (6, 3)
                            relief='flat',
                            borderwidth=1,
                            focuscolor='none',
                            font=('Arial', 8))
        
        self.style.map('Small.TButton',
                      background=[('active', '#AFEEEE'),   # Pale turquoise
                                 ('pressed', '#ADD8E6')],  # Light blue
                      relief=[('pressed', 'flat'),
                             ('!pressed', 'flat')])
        
        # Secondary button style for less important actions
        self.style.configure('Secondary.TButton',
                            background='#E0F6FF',  # Very light blue
                            foreground='black',
                            padding=(6, 3),
                            relief='flat',
                            borderwidth=1,
                            focuscolor='none',
                            font=('Arial', 8))
        
        self.style.map('Secondary.TButton',
                      background=[('active', '#D0EFFF'),
                                 ('pressed', '#C0E8FF')],
                      relief=[('pressed', 'flat'),
                             ('!pressed', 'flat')])
        
        # Success button style for save/generate actions
        self.style.configure('Success.TButton',
                            background='#87CEEB',  # Sky blue
                            foreground='black',
                            padding=(6, 3),
                            relief='flat',
                            borderwidth=1,
                            focuscolor='none',
                            font=('Arial', 8, 'bold'))
        
        self.style.map('Success.TButton',
                      background=[('active', '#87CEFA'),   # Light sky blue
                                 ('pressed', '#ADD8E6')],  # Light blue
                      relief=[('pressed', 'flat'),
                             ('!pressed', 'flat')])

    def setup_preview_styles(self):
        """Setup enhanced styles for the data preview"""
        # Configure modern Treeview styling
        self.style.configure('Preview.Treeview',
                            background='#FFFFFF',
                            foreground='#2C3E50',
                            fieldbackground='#FFFFFF',
                            borderwidth=1,
                            relief='solid',
                            font=('Arial', 9))
        
        self.style.configure('Preview.Treeview.Heading',
                            background='#3498DB',
                            foreground='black',
                            font=('Arial', 9, 'bold'),
                            relief='flat',
                            borderwidth=1)
        
        # Configure alternating row colors
        self.style.map('Preview.Treeview',
                      background=[('selected', '#E8F4FD')],
                      foreground=[('selected', '#2C3E50')])

    def get_file_size_mb(self, file_path):
        """Get file size in megabytes"""
        try:
            size_bytes = os.path.getsize(file_path)
            size_mb = size_bytes / (1024 * 1024)  # Convert to MB
            return size_mb
        except Exception:
            return 0

    def format_file_size(self, size_mb):
        """Format file size for display"""
        if size_mb < 1:
            return f"{size_mb * 1024:.1f} KB"
        elif size_mb < 1024:
            return f"{size_mb:.1f} MB"
        else:
            return f"{size_mb / 1024:.2f} GB"

    def update_large_file_indicator(self, file_path=None):
        """Show or hide the large file indicator based on file size"""
        # Clear existing indicator
        if self.large_file_indicator:
            self.large_file_indicator.destroy()
            self.large_file_indicator = None
        
        # If no file path provided, just clear the indicator
        if not file_path:
            return
        
        # Check file size
        size_mb = self.get_file_size_mb(file_path)
        
        # Show indicator if file exceeds threshold
        if size_mb >= self.large_file_threshold_mb:
            # Find the preview frame to add the indicator to
            try:
                # Look for the preview_frame (which contains the preview controls)
                for widget in self.master.winfo_children():
                    if isinstance(widget, tk.Frame):
                        for child in widget.winfo_children():
                            if isinstance(child, tk.LabelFrame) and "Select Source File" in child.cget('text'):
                                # Find the preview controls frame within the file group
                                for subchild in child.winfo_children():
                                    if isinstance(subchild, tk.Frame):
                                        # Check if this frame contains preview controls
                                        frame_children = subchild.winfo_children()
                                        if len(frame_children) > 0:
                                            # Look for a frame that contains preview controls
                                            for frame_child in frame_children:
                                                if isinstance(frame_child, tk.Frame):
                                                    # This should be the preview_controls frame
                                                    preview_controls_parent = subchild
                                                    
                                                    # Create the large file indicator
                                                    size_text = self.format_file_size(size_mb)
                                                    indicator_text = f"⚠️ Large File ({size_text})"
                                                    
                                                    self.large_file_indicator = tk.Label(
                                                        preview_controls_parent, 
                                                        text=indicator_text,
                                                        font=('Arial', 8), 
                                                        bg='#D1ECF1',  # Light blue background
                                                        fg='#0C5460',  # Dark blue text
                                                        relief='solid', 
                                                        bd=1, 
                                                        padx=6, 
                                                        pady=2
                                                    )
                                                    self.large_file_indicator.pack(side='right', padx=10, pady=6)
                                                    return
            except Exception as e:
                print(f"Error creating large file indicator: {e}")
  
    def build_file_selection_screen(self):
        self.master.iconbitmap(resource_path('sqlbuilder_icon.ico'))  # This sets the icon
        self.delimiter.set("")  # Clear the delimiter field
        self.file_path.set("")  # Clear previously selected file
        self.data_cache.clear()  # Clear cached data
        self.master.geometry("950x600")
        for widget in self.master.winfo_children():
            widget.destroy()

        # Get the window background color safely
        try:
            bg_color = str(self.master.cget("bg"))
            safe_menu_opts = {'background': bg_color, 'activebackground': bg_color}
        except Exception:
            safe_menu_opts = {}

        # Create the main menu bar
        menubar = tk.Menu(self.master, **safe_menu_opts)
        self.master.config(menu=menubar)

        # File menu
        file_menu = tk.Menu(menubar, tearoff=0, **safe_menu_opts)
        file_menu.add_command(label="Open", command=self.browse_file, accelerator="Ctrl+O")
        file_menu.add_separator() 
        file_menu.add_command(label="Exit", command=self.safe_exit, accelerator="Ctrl+E")
        menubar.add_cascade(label="File", menu=file_menu)
        
        # Edit menu
        edit_menu = tk.Menu(menubar, tearoff=0, **safe_menu_opts)
        edit_menu.add_command(label="Settings", command=lambda: self.config_mgr.open_settings_window(self.master, self.apply_config_settings), accelerator="Ctrl+S")
        menubar.add_cascade(label="Edit", menu=edit_menu)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0, **safe_menu_opts)
        help_menu.add_command(label="Documentation", command=self.open_github_repository, accelerator="Ctrl+D")
        menubar.add_cascade(label="Help", menu=help_menu)

        # Bind keyboard shortcuts
        self.master.bind_all('<Control-o>', lambda e: self.browse_file())
        self.master.bind_all('<Control-O>', lambda e: self.browse_file())
        self.master.bind_all('<Control-e>', lambda e: self.safe_exit())
        self.master.bind_all('<Control-E>', lambda e: self.safe_exit())
        self.master.bind_all('<Control-s>', lambda e: self.config_mgr.open_settings_window(self.master, self.apply_config_settings))
        self.master.bind_all('<Control-S>', lambda e: self.config_mgr.open_settings_window(self.master, self.apply_config_settings))
        self.master.bind_all('<Control-d>', lambda e: self.open_github_repository())
        self.master.bind_all('<Control-D>', lambda e: self.open_github_repository())

        main_frame = tk.Frame(self.master, padx=20, pady=20)
        main_frame.pack(expand=True, fill="both")
       
        # File selection group
        file_group = tk.LabelFrame(main_frame, text="Select Source File", padx=10, pady=10)
        file_group.pack(fill="x", pady=5)

        # File path selection row
        file_frame = tk.Frame(file_group)
        file_frame.pack(fill="x", pady=5)
        file_entry = tk.Entry(file_frame, textvariable=self.file_path, width=50)
        file_entry.pack(side="left", fill="x", expand=True, padx=(0, 5))
        ttk.Button(file_frame, text="Browse...", 
                  style='Primary.TButton',
                  command=self.browse_file).pack(side="left")

        # Preview controls row (removed delimiter controls)
        preview_frame = tk.Frame(file_group)
        preview_frame.pack(fill="x", pady=(10, 10))
        
        # Preview controls (moved to left side since delimiter controls are removed)
        preview_controls = tk.Frame(preview_frame)
        preview_controls.pack(side="left")
        
        # Preview percentage with label and entry
        tk.Label(preview_controls, text="Preview %", font=('Arial', 9)).pack(side="left")
        preview_entry = tk.Entry(preview_controls, textvariable=self.preview_percentage_var, width=4, justify="center")
        preview_entry.pack(side="left", padx=(5, 2))
        
        # Show button
        self.show_button = ttk.Button(preview_controls, text="Preview Data", 
                                     style='Small.TButton',
                                     width=12, state="disabled", 
                                     command=self.on_apply_preview_percentage)
        self.show_button.pack(side="left", padx=(8, 0))
        
        # Note: Large file indicator will be added dynamically to preview_frame
                
        # Action button
        self.preview_frame = tk.LabelFrame(main_frame, text="Data Preview", padx=5, pady=5)
        self.preview_frame.pack(fill="both", expand=True, pady=(10, 10))

        action_frame = tk.Frame(main_frame)
        action_frame.pack(pady=15, fill="x")
        
        # Settings button - positioned to the left
        ttk.Button(action_frame, text="Settings", 
                  style='Secondary.TButton',
                  width=8, 
                  command=lambda: self.config_mgr.open_settings_window(self.master, self.apply_config_settings)).pack(side="left")
        
        # Clear button - disabled until file is selected
        self.clear_button = ttk.Button(action_frame, text="Clear", 
                                      style='Secondary.TButton',
                                      width=8, state="disabled",
                                      command=self.clear_data)
        self.clear_button.pack(side="left", padx=(10, 0))
        
        # Exit button - moved to middle position
        ttk.Button(action_frame, text="Exit", 
                  style='Secondary.TButton',
                  width=8, 
                  command=self.safe_exit).pack(side="left", padx=(10, 0))
        
        # Next button - moved to the right side
        self.next_button = ttk.Button(action_frame, text="Next →", 
                                     style='Primary.TButton',
                                     width=8, state="disabled", 
                                     command=self.process_file)
        self.next_button.pack(side="right")
                
    def browse_file(self):
        # Updated to include JSON files
        filetypes = [("Data Files", "*.csv *.txt *.dat *.json"), ("All Files", "*.*")]
        selected_path = filedialog.askopenfilename(title="Open File", filetypes=filetypes)
        if selected_path:
            self.file_path.set(selected_path)
            
            # Determine file type and handle accordingly
            file_type = self.data_cache.get_file_type(selected_path)
            
            if file_type == 'json':
                # Skip delimiter inference for JSON files
                self.delimiter.set("N/A (JSON)")
            else:
                # Infer delimiter for CSV files
                self.infer_delimiter()
            
            # Set table name based on configuration
            self.set_table_name_from_config(selected_path)
            
            # Clear any existing cached data when new file is selected
            self.data_cache.clear()
            
            # Update large file indicator
            self.update_large_file_indicator(selected_path)
            
            self.next_button.config(state="normal")
            self.show_button.config(state="normal")
            self.clear_button.config(state="normal")  # Enable Clear button when file is selected
            
            # Clear any existing preview
            for widget in self.preview_frame.winfo_children():
                widget.destroy()
            
            # NEW: Automatically preview data if auto_preview_data is enabled
            if self.auto_preview_data:
                self.on_apply_preview_percentage()

    def set_table_name_from_config(self, file_path):
        """Set table name based on configuration settings"""
        if self.use_filename_as_table_name:
            # Use filename as table name (current behavior)
            default_name = os.path.splitext(os.path.basename(file_path))[0]
            self.table_name.set(default_name)
        else:
            # Use custom table name from config
            self.table_name.set(self.custom_table_name)

    def infer_delimiter(self):
        possible_delimiters = [',', '|', '\t', ';', ':', '^']
        try:
            with open(self.file_path.get(), 'r') as f:
                sample = f.readline()
                counts = {delim: sample.count(delim.replace('\t', '\t')) for delim in possible_delimiters}
                likely_delim = max(counts, key=counts.get)
                self.delimiter.set(likely_delim if likely_delim != '\t' else '\\t')
        except Exception as e:
            self.delimiter.set(',')

    def clear_data(self):
        """Clear all loaded data and reset the interface"""
        # Clear file path and data
        self.file_path.set("")
        self.delimiter.set("")
        self.table_name.set("")
        
        # Clear cached data
        self.data_cache.clear()
        
        # Clear large file indicator
        self.update_large_file_indicator()
        
        # Clear preview display
        for widget in self.preview_frame.winfo_children():
            widget.destroy()
        
        # Disable buttons until new file is selected
        self.show_button.config(state="disabled")
        self.next_button.config(state="disabled")
        self.clear_button.config(state="disabled")  # Disable Clear button after clearing

    def process_file(self):
        """Process file with progress dialog and caching"""
        path = self.file_path.get()
        if not path:
            return
            
        # Show progress dialog
        progress = ProgressWindow(self.master, "Loading File...")
        
        def load_file_task():
            try:
                progress.update_text("Reading file structure...")
                
                delimiter_val = self.delimiter.get()
                # Only process delimiter for non-JSON files
                if delimiter_val != "N/A (JSON)":
                    delimiter_val = "\t" if delimiter_val == "\\t" else delimiter_val
                else:
                    delimiter_val = None  # Not used for JSON
                
                # Load file into cache
                progress.update_text("Loading data into cache...")
                self.data_cache.load_file(path, delimiter_val, self.sample_percentage)
                
                if progress.cancelled:
                    return
                    
                self.headers = self.data_cache.headers
                # Store original headers for "Source File" reset functionality
                self.original_headers = self.headers.copy()
                
                progress.update_text("Processing complete!")
                time.sleep(0.5)  # Brief pause to show completion
                
                # Schedule UI update on main thread
                self.master.after(0, lambda: [progress.close(), self.build_column_type_screen()])
                
            except Exception as e:
                self.master.after(0, lambda: [progress.close(), messagebox.showerror("Error", f"Failed to process file: {e}")])
        
        # Run file loading in background thread
        self.executor.submit(load_file_task)
      
    def build_column_type_screen(self):
        self.additional_column_count = 0
        self.master.geometry("550x800")
        for widget in self.master.winfo_children():
            widget.destroy()

        # === TABLE SETTINGS SECTION ===
        settings_frame = tk.LabelFrame(self.master, text="Table Configuration", padx=10, pady=10)
        settings_frame.pack(fill="x", padx=10, pady=10)

        # Database
        db_frame = tk.Frame(settings_frame)
        db_frame.pack(fill="x", pady=3)
        tk.Label(db_frame, text="Database:", width=12, anchor="w").pack(side="left")
        tk.Entry(db_frame, width=40, textvariable=self.database_name).pack(side="left")

        # Schema
        schema_frame = tk.Frame(settings_frame)
        schema_frame.pack(fill="x", pady=3)
        tk.Label(schema_frame, text="Schema:", width=12, anchor="w").pack(side="left")
        tk.Entry(schema_frame, width=40, textvariable=self.schema_name).pack(side="left")

        # Table
        table_frame = tk.Frame(settings_frame)
        table_frame.pack(fill="x", pady=3)
        tk.Label(table_frame, text="Table:", width=12, anchor="w").pack(side="left")
        tk.Entry(table_frame, width=40, textvariable=self.table_name).pack(side="left")

        # Row for renaming dropdown
        rename_frame = tk.Frame(settings_frame)
        rename_frame.pack(fill="x", pady=3)
        tk.Label(rename_frame, text="Column Format:").pack(side="left", padx=(5, 2))
        self.naming_style_var = tk.StringVar(value=self.config_mgr.config.get("default_column_format", "Source File"))
        # Added "Source File" option to the dropdown values
        self.naming_combo = ttk.Combobox(rename_frame, textvariable=self.naming_style_var, 
                                        values=["Source File", "CamelCase", "snake_case", "lowercase", "UPPERCASE"], 
                                        width=15, state="readonly")
        self.naming_combo.pack(side="left", padx=2)
        
        # Bind the dropdown selection to automatically apply the formatting
        self.naming_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_column_naming_convention())
        
    

        # === COLUMN DEFINITION SECTION ===
        column_frame = tk.LabelFrame(self.master, text="Define Table Columns", padx=10, pady=10)
        column_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Add Column and Remove Column buttons at the top of the frame
        column_buttons_frame = tk.Frame(column_frame)
        column_buttons_frame.pack(fill="x", pady=(0, 10))
        
        self.add_column_button = ttk.Button(column_buttons_frame, text="Add Column", 
                                           style='Small.TButton',
                                           command=self.add_new_column_row)
        self.add_column_button.pack(side="left", padx=(0, 5))
        
        self.remove_column_button = ttk.Button(column_buttons_frame, text="Remove Column", 
                                              style='Small.TButton',
                                              command=self.remove_last_column,
                                              state="disabled")
        self.remove_column_button.pack(side="left")
        
        self.reset_button = ttk.Button(column_buttons_frame, text="Reset Types", 
                                      style='Small.TButton',
                                      command=self.reset_data_types_immediately, 
                                      state="disabled")
        self.reset_button.pack(side="left", padx=(15, 0))

        canvas = tk.Canvas(column_frame, highlightthickness=0)  # Remove focus highlight
        scrollbar = tk.Scrollbar(column_frame, orient="vertical", command=canvas.yview)
        self.scrollable_frame = tk.Frame(canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Disable canvas focus to prevent bold frame highlighting
        canvas.bind('<Button-1>', lambda e: self.master.focus_set())

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        header_row = tk.Frame(self.scrollable_frame)
        header_row.pack(fill="x", pady=2)
        tk.Label(header_row, text="🔑", width=3, anchor="w").pack(side="left")
        tk.Label(header_row, text="Column Name", width=26, anchor="w").pack(side="left")
        tk.Label(header_row, text="Data Type", width=26, anchor="w").pack(side="left")
        tk.Label(header_row, text="Null", width=5, anchor="w").pack(side="left")

        self.column_entries.clear()
        self.type_entries.clear()
        self.pk_vars.clear()
        self.pk_checkboxes.clear()
        self.null_vars.clear()
        self.null_checkboxes.clear()

        for index, header in enumerate(self.headers):
            row = tk.Frame(self.scrollable_frame)
            row.pack(fill="x", pady=1, anchor="w")

            pk_var = tk.BooleanVar()
            null_var = tk.BooleanVar(value=True)

            pk_checkbox = tk.Checkbutton(row, variable=pk_var, command=lambda idx=index: self.update_pk_states(idx))
            pk_checkbox.pack(side="left")

            name_entry = tk.Entry(row, width=30)
            name_entry.insert(0, header)
            name_entry.pack(side="left")

            type_combo = ttk.Combobox(row, width=27, values=self.sql_data_types)
            type_combo.pack(side="left", padx=5)
            
            # Simple event bindings that actually work
            type_combo.bind("<<ComboboxSelected>>", lambda e, idx=index: self.enable_reset_button(idx))
            type_combo.bind("<KeyRelease>", lambda e, idx=index: self.enable_reset_button(idx))
            type_combo.bind("<Button-1>", lambda e, idx=index: self.enable_reset_button(idx))

            null_checkbox = tk.Checkbutton(row, variable=null_var, command=lambda idx=index: self.update_null_states(idx))
            null_checkbox.pack(side="left")

            self.pk_vars.append(pk_var)
            self.null_vars.append(null_var)
            self.pk_checkboxes.append(pk_checkbox)
            self.null_checkboxes.append(null_checkbox)
            self.column_entries.append(name_entry)
            self.type_entries.append(type_combo)

        if self.infer_types_var.get():
            self.set_inferred_types_async()

        # Automatically apply the default column format
        self.apply_column_naming_convention()

        # === SCRIPT GENERATOR SECTION ===
        script_frame = tk.LabelFrame(self.master, text="Script Generator", padx=10, pady=10)
        script_frame.pack(fill="x", padx=10, pady=(10, 0))
        
        checkbox_row = tk.Frame(script_frame)
        checkbox_row.pack(fill="x", pady=5)

        tk.Checkbutton(checkbox_row, text="CREATE TABLE", variable=self.include_create_script).pack(side="left", padx=10)
        insert_checkbox = tk.Checkbutton(checkbox_row, text="INSERT INTO", variable=self.include_insert_script)
        insert_checkbox.pack(side="left", padx=10)
        self.include_insert_script.trace_add("write", self.update_truncate_enable_state)
        self.batch_insert_check = tk.Checkbutton(checkbox_row, text=f"Batch INSERT ({self.insert_batch_size})", variable=self.batch_insert_var)
        self.batch_insert_check.pack(side="left", padx=5)
        self.truncate_check = tk.Checkbutton(checkbox_row, text="TRUNCATE", variable=self.truncate_before_insert, command=self.update_truncate_color)
        self.truncate_check.pack(side="left", padx=10)
        
        # File info display (rows count) - positioned beneath Batch INSERT
        if self.data_cache.is_loaded:
            info_frame = tk.Frame(script_frame)
            info_frame.pack(fill="x", pady=(5, 0))
            file_info = self.data_cache.file_info
            
            # Get the full filename from the file path
            file_path = file_info.get('file_path', '')
            filename = os.path.basename(file_path) if file_path else "Unknown"
            
            # Display different info based on file type
            if file_info.get('file_type') == 'json':
                rows_text = f"{file_info['total_rows']:,}"
            else:
                rows_text = f"~{file_info['estimated_rows']:,}" if file_info['is_large_file'] else f"{file_info['total_rows']:,}"
            
            # Row count label
            tk.Label(info_frame, text=f"Total Rows: {rows_text}", fg="black", font=('Arial', 9)).pack(side="left", padx=(10, 2))
            
            # Filename label on the same line
            tk.Label(info_frame, text=f"| {filename}", fg="gray", font=('Arial', 9)).pack(side="left", padx=(2, 10))
        
        self.update_truncate_enable_state()
        self.update_truncate_color()  # Ensure initial color is set correctly

        # === BACK AND EXIT BUTTONS ===

        back_frame = tk.Frame(self.master)
        back_frame.pack(pady=(5, 15))
        ttk.Button(back_frame, text="← Back", 
                  style='Secondary.TButton',
                  width=8, 
                  command=self.build_file_selection_screen).pack(side="left", padx=10)
        ttk.Button(back_frame, text="Save", 
                  style='Success.TButton',
                  width=9, 
                  command=self.handle_generate_scripts).pack(side="left", padx=10)
        ttk.Button(back_frame, text="Exit", 
                  style='Secondary.TButton',
                  width=8, 
                  command=self.safe_exit).pack(side="left", padx=10)

    def handle_generate_scripts(self):
        create_file_path = None
        
        if self.include_create_script.get():
            create_file_path = self.generate_sql_file()
        if self.include_insert_script.get():
            self.generate_insert_statements_optimized(create_file_path)

    def add_pk_options_to_dropdown(self, index):
        """Add INT IDENTITY and UNIQUEIDENTIFIER to the top of a dropdown when PK is selected"""
        type_combo = self.type_entries[index]
        current_values = list(type_combo['values'])
        
        # Remove these options if they already exist
        pk_options = ["INT IDENTITY", "UNIQUEIDENTIFIER"]
        for option in pk_options:
            if option in current_values:
                current_values.remove(option)
        
        # Add them to the top
        new_values = pk_options + current_values
        type_combo['values'] = new_values

    def remove_pk_options_from_dropdown(self, index):
        """Remove INT IDENTITY and UNIQUEIDENTIFIER from dropdown when PK is unselected"""
        type_combo = self.type_entries[index]
        current_values = list(type_combo['values'])
        current_value = type_combo.get()
        
        # Remove PK-specific options
        pk_options = ["INT IDENTITY", "UNIQUEIDENTIFIER"]
        new_values = [val for val in current_values if val not in pk_options]
        type_combo['values'] = new_values
        
        # If current selection was a PK-specific type, clear it
        if current_value in pk_options:
            type_combo.delete(0, tk.END)

    def update_pk_states(self, selected_index):
        # First, handle mutual exclusivity (only one PK allowed)
        for i, (pk_var, pk_checkbox, null_var, null_checkbox) in enumerate(
            zip(self.pk_vars, self.pk_checkboxes, self.null_vars, self.null_checkboxes)
        ):
            if self.pk_vars[selected_index].get():
                # A PK was selected
                if i != selected_index:
                    # Disable all other PK checkboxes and remove their PK options
                    pk_var.set(False)
                    pk_checkbox.config(state="disabled")
                    self.remove_pk_options_from_dropdown(i)
                else:
                    # This is the selected PK column
                    pk_checkbox.config(state="normal")
                    null_var.set(False)
                    null_checkbox.config(state="disabled")
                    # Add PK options to this dropdown
                    self.add_pk_options_to_dropdown(selected_index)
            else:
                # No PK selected, enable all checkboxes and remove PK options from all dropdowns
                pk_checkbox.config(state="normal")
                null_checkbox.config(state="normal")
                self.remove_pk_options_from_dropdown(i)

    def update_null_states(self, selected_index):
        for i, (pk_var, pk_checkbox, null_var, null_checkbox) in enumerate(
            zip(self.pk_vars, self.pk_checkboxes, self.null_vars, self.null_checkboxes)
        ):
            if null_var.get():
                pk_var.set(False)
                pk_checkbox.config(state="disabled")
                # Remove PK options when null is allowed
                self.remove_pk_options_from_dropdown(i)
            else:
                pk_checkbox.config(state="normal")

    def update_truncate_color(self):
        if self.truncate_before_insert.get():
            self.truncate_check.config(fg="red")
        else:
            self.truncate_check.config(fg="black")
    
    def update_truncate_enable_state(self, *args):
        try:
            if not self.include_insert_script.get():
                if self.truncate_check.winfo_exists():
                    self.truncate_check.config(state="disabled")
                if self.batch_insert_check.winfo_exists():
                    self.batch_insert_check.config(state="disabled")
            else:
                if self.truncate_check.winfo_exists():
                    self.truncate_check.config(state="normal")
                if self.batch_insert_check.winfo_exists():
                    self.batch_insert_check.config(state="normal")
        except AttributeError:
            pass

    def enable_reset_button(self, column_index=None):
        """Enable the reset button only if an original column is modified"""
        if hasattr(self, 'reset_button') and self.reset_button.winfo_exists():
            # Only enable if this is an original column (not manually added)
            if column_index is not None and hasattr(self, 'headers'):
                if column_index < len(self.headers):  # Original column
                    self.reset_button.config(state="normal")
                # If it's a manually added column, don't enable the reset button
            else:
                # Fallback for cases where index isn't provided
                self.reset_button.config(state="normal")

    def generate_sql_file(self):
        table_name = self.table_name.get().strip()
        schema_name = self.schema_name.get().strip()
        full_table = f"[{schema_name}].[{table_name}]"
        if not table_name:
            return None

        db_name = self.database_name.get().strip()
        create_lines = []

        if db_name:
            create_lines.append(f"USE [{db_name}];")
            create_lines.append("GO")
            create_lines.append("")

        create_lines.append(f"CREATE TABLE {full_table} (")

        pk_columns = []

        for i, (col_entry, type_entry, pk_var, null_var) in enumerate(zip(self.column_entries, self.type_entries, self.pk_vars, self.null_vars)):
            col_name = col_entry.get()
            col_type = type_entry.get()
            null_str = "NULL" if null_var.get() else "NOT NULL"
            column_def = f"    [{col_name}] {col_type} {null_str}"
            if i < len(self.column_entries) - 1 or any(var.get() for var in self.pk_vars):
                column_def += ","
            create_lines.append(column_def)
            if pk_var.get():
                pk_columns.append(col_name)

        if pk_columns:
            constraint_name = f"PK_{table_name}"
            pk_line = f"    CONSTRAINT {constraint_name} PRIMARY KEY ({', '.join(f'[{col}]' for col in pk_columns)})"
            create_lines.append(pk_line)

        create_lines.append(");")
        script = "\n".join(create_lines)

        default_filename = f"create_table_{table_name}.sql"
        file_path = filedialog.asksaveasfilename(defaultextension=".sql", initialfile=default_filename, filetypes=[("SQL Files", "*.sql")])
        if file_path:
            with open(file_path, 'w') as f:
                f.write(script)
            return file_path
        return None

    def generate_insert_statements_optimized(self, create_file_path=None):
        """Optimized insert statement generation with chunked processing and progress tracking"""
        table_name = self.table_name.get().strip()
        schema_name = self.schema_name.get().strip()
        full_table = f"[{schema_name}].[{table_name}]"
        if not table_name:
            return
        
        col_names = []
        column_types = []
        for entry, dtype in zip(self.column_entries, self.type_entries):
            col_name = entry.get().strip()
            col_type = dtype.get().strip().upper()
            if "INT IDENTITY" not in col_type:
                col_names.append(col_name)
                column_types.append(col_type)

        # Get save location first
        default_filename = f"insert_into_{table_name}.sql"
        file_path = filedialog.asksaveasfilename(defaultextension=".sql", initialfile=default_filename, filetypes=[("SQL Files", "*.sql")])
        if not file_path:
            return

        # Always show progress dialog for INSERT generation
        progress = ProgressWindow(self.master, "Generating INSERT Statements...")
        
        def generate_task():
            try:
                progress.update_text("Initializing INSERT script generation...")
                
                script_lines = []
                db_name = self.database_name.get().strip()

                if db_name:
                    script_lines.append(f"USE [{db_name}];")
                    script_lines.append("GO")
                    script_lines.append("")

                if self.truncate_before_insert.get():
                    script_lines.append(f"TRUNCATE TABLE {full_table};")
                    script_lines.append("GO")
                    script_lines.append("")

                insert_header = f"INSERT INTO {full_table} ({', '.join(f'[{c}]' for c in col_names)})\nVALUES"
                
                progress.update_text("Reading and processing data...")
                
                # Process data in chunks
                all_values = []
                chunk_count = 0
                total_rows_processed = 0
                
                for chunk in self.data_cache.get_chunk_generator():
                    if progress.cancelled:
                        return
                        
                    chunk_count += 1
                    progress.update_text(f"Processing data chunk {chunk_count}...")
                    
                    for row in chunk:
                        # Pad row if necessary
                        extra_count = len(column_types) - len(row)
                        if extra_count > 0:
                            row = [''] * extra_count + row
                        all_values.append(self.format_insert_values(row, column_types))
                        total_rows_processed += 1
                        
                        # Update progress every 1000 rows for better user feedback
                        if total_rows_processed % 1000 == 0:
                            progress.update_text(f"Processed {total_rows_processed:,} rows...")
                
                if progress.cancelled:
                    return
                    
                # Generate final script
                progress.update_text(f"Generating SQL script for {total_rows_processed:,} rows...")
                
                if self.batch_insert_var.get():
                    batch_count = 0
                    total_batches = (len(all_values) + self.insert_batch_size - 1) // self.insert_batch_size
                    
                    for i in range(0, len(all_values), self.insert_batch_size):
                        if progress.cancelled:
                            return
                            
                        batch_count += 1
                        progress.update_text(f"Creating batch {batch_count} of {total_batches}...")
                        
                        chunk = all_values[i:i + self.insert_batch_size]
                        script_lines.append(insert_header)
                        script_lines.append(",\n".join(chunk) + ";\nGO")
                else:
                    script_lines.append(insert_header)
                    script_lines.append(",\n".join(all_values) + ";\nGO")

                script = "\n".join(script_lines)
                
                progress.update_text("Saving file to disk...")
                
                with open(file_path, 'w') as f:
                    f.write(script)
                
                # Calculate file size for display
                file_size = os.path.getsize(file_path)
                file_size_mb = file_size / (1024 * 1024)
                
                # Build completion message
                completion_msg_parts = []
                
                # Include CREATE TABLE info if it was generated
                if create_file_path:
                    completion_msg_parts.append("✅ CREATE TABLE script successfully created!")
                    completion_msg_parts.append(f"📁 File: {os.path.basename(create_file_path)}")
                    completion_msg_parts.append("")  # Blank line
                
                # Add INSERT script info
                completion_msg_parts.append("✅ INSERT script successfully created!")
                completion_msg_parts.append(f"📁 File: {os.path.basename(file_path)}")
                completion_msg_parts.append(f"📊 Rows: {total_rows_processed:,}  💾 Size: {file_size_mb:.1f} MB")
                
                completion_msg = "\n".join(completion_msg_parts)
                
                # Schedule UI update on main thread to show completion info on progress window
                self.master.after(0, lambda: [
                    progress.update_text(completion_msg),
                    progress.show_completion(dual_scripts=bool(create_file_path))
                ])
                    
            except Exception as e:
                error_msg = f"❌ Failed to generate INSERT statements:\n\n{str(e)}"
                self.master.after(0, lambda: [
                    progress.update_text(error_msg),
                    progress.show_completion(dual_scripts=False)
                ])

        # Always run generation in background thread
        self.executor.submit(generate_task)

    def reset_data_types_immediately(self):
        """Reset data types immediately without progress window"""
        if not self.data_cache.is_loaded:
            return
            
        try:
            # Use cached sample data for type inference
            inferred_types = self.type_inferrer.infer_column_types(
                self.data_cache.sample_rows, 
                self.headers
            )
            
            # Update UI immediately - only reset types for original columns (not added ones)
            original_column_count = len(self.headers)
            for i, inferred_type in enumerate(inferred_types):
                if i < original_column_count and i < len(self.type_entries):
                    combo = self.type_entries[i]
                    combo.delete(0, "end")
                    combo.insert(0, inferred_type)
            
            # Leave manually added columns' data types unchanged
            
            # Disable reset button after resetting
            self.reset_button.config(state="disabled")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to reset data types: {e}")

    def set_inferred_types_async(self):
        """Asynchronously infer types with progress indication"""
        if not self.data_cache.is_loaded:
            return
            
        progress = ProgressWindow(self.master, "Inferring Data Types...")
        
        def infer_task():
            try:
                progress.update_text("Analyzing data patterns...")
                
                # Use cached sample data for type inference
                inferred_types = self.type_inferrer.infer_column_types(
                    self.data_cache.sample_rows, 
                    self.headers
                )
                
                if progress.cancelled:
                    return
                    
                progress.update_text("Updating column types...")
                
                # Schedule UI update on main thread
                def update_ui():
                    try:
                        # Only update types for original columns (not added ones)
                        original_column_count = len(self.headers)
                        for i, inferred_type in enumerate(inferred_types):
                            if i < original_column_count and i < len(self.type_entries):
                                combo = self.type_entries[i]
                                combo.delete(0, "end")
                                combo.insert(0, inferred_type)
                        
                        # Leave manually added columns' data types unchanged
                        
                        # Enable the reset button after type inference (initial or user-initiated)
                        self.reset_button.config(state="normal")
                        if not hasattr(self, 'initial_type_inference_done'):
                            self.initial_type_inference_done = True
                        self.reset_button.config(state="disabled")
                        
                        progress.close()
                        
                    except Exception as e:
                        progress.close()
                        messagebox.showerror("Error", f"Failed to update types: {e}")
                
                self.master.after(0, update_ui)
                
            except Exception as e:
                self.master.after(0, lambda: [progress.close(), messagebox.showerror("Error", f"Failed to infer types: {e}")])
        
        # Run type inference in background thread
        self.executor.submit(infer_task)

    def set_inferred_types(self):
        """Legacy method - now redirects to async version"""
        self.set_inferred_types_async()

    def toggle_infer_types(self):
        if self.infer_types_var.get():
            self.set_inferred_types_async()
        else:
            for combo in self.type_entries:
                combo.delete(0, "end")

    def format_column_name(self, name: str, style: str) -> str:
        import re
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

    def apply_column_naming_convention(self):
        style = self.naming_style_var.get()
        
        if style == "Source File":
            # Reset column names back to original source file names
            # Only reset original columns, not manually added ones
            original_column_count = len(self.original_headers)
            for i, entry in enumerate(self.column_entries):
                if i < original_column_count:
                    # This is an original column, reset to source name
                    entry.delete(0, "end")
                    entry.insert(0, self.original_headers[i])
                # Leave manually added columns unchanged
        else:
            # Apply formatting to all columns
            for entry in self.column_entries:
                old_name = entry.get()
                new_name = self.format_column_name(old_name, style)
                entry.delete(0, "end")
                entry.insert(0, new_name)
        
        self.master.focus()

    def get_delimiter_display_name(self, delimiter):
        """Convert delimiter character to descriptive display name"""
        delimiter_map = {
            ',': 'comma ( , )',
            '|': 'pipe ( | )',
            '\t': 'tab ( \\t )',
            '\\t': 'tab ( \\t )',  # Handle escaped tab representation
            ';': 'semicolon ( ; )',
            ':': 'colon ( : )',
            '*': 'asterisk ( * )',
            '^': 'caret ( ^ )',
            ' ': 'space (   )',
            '#': 'hash ( # )',
            '~': 'tilde ( ~ )',
            '@': 'at ( @ )',
            '!': 'exclamation ( ! )',
            '%': 'percent ( % )',
            '&': 'ampersand ( & )',
            '+': 'plus ( + )',
            '=': 'equals ( = )',
            '/': 'slash ( / )',
            '\\': 'backslash ( \\ )',
            '-': 'dash ( - )',
            '_': 'underscore ( _ )'
        }
        
        if delimiter == "":
            return "auto-detected"
        elif delimiter == "N/A (JSON)":
            return "N/A (JSON file)"
        elif delimiter in delimiter_map:
            return delimiter_map[delimiter]
        else:
            return f"other ( {delimiter} )" 

    def on_apply_preview_percentage(self):
        try:
            percent = int(self.preview_percentage_var.get())
            if 1 <= percent <= 100:
                # Load file data if not already loaded
                if not self.data_cache.is_loaded:
                    self.load_file_for_preview(percent)
                else:
                    self.update_preview_table(percentage=percent)
            else:
                raise ValueError
        except ValueError:
            messagebox.showerror("Invalid Input", "Please enter an integer between 1 and 100.")
    
    def load_file_for_preview(self, preview_percent):
        """Load file data specifically for preview functionality"""
        path = self.file_path.get()
        if not path:
            return
            
        # Show progress dialog for file loading
        progress = ProgressWindow(self.master, "Loading File for Preview...")
        
        def load_preview_task():
            try:
                progress.update_text("Reading file...")
                
                delimiter_val = self.delimiter.get()
                # Handle delimiter for non-JSON files
                if delimiter_val != "N/A (JSON)":
                    delimiter_val = "\t" if delimiter_val == "\\t" else delimiter_val
                else:
                    delimiter_val = None  # Not used for JSON
                
                # Load file into cache
                self.data_cache.load_file(path, delimiter_val, self.sample_percentage)
                
                if progress.cancelled:
                    return
                
                # Update headers
                self.headers = self.data_cache.headers
                
                progress.update_text("Generating preview...")
                
                # Schedule UI update on main thread
                self.master.after(0, lambda: [
                    progress.close(), 
                    self.update_preview_table(percentage=preview_percent)
                ])
                
            except Exception as e:
                self.master.after(0, lambda: [
                    progress.close(), 
                    messagebox.showerror("Error", f"Failed to load file for preview: {e}")
                ])
        
        # Run file loading in background thread
        self.executor.submit(load_preview_task)

    def update_preview_table(self, percentage=1):
        """Enhanced preview table with modern styling and visual improvements"""
        for widget in self.preview_frame.winfo_children():
            widget.destroy()

        if not self.data_cache.is_loaded:
            # Enhanced no-data message
            no_data_frame = tk.Frame(self.preview_frame, bg='#F8F9FA', relief='solid', bd=1)
            no_data_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            icon_label = tk.Label(no_data_frame, text="📊", font=('Arial', 24), bg='#F8F9FA', fg='#6C757D')
            icon_label.pack(pady=(20, 5))
            
            message_label = tk.Label(no_data_frame, text="No data loaded", 
                                   font=('Arial', 12, 'bold'), bg='#F8F9FA', fg='#495057')
            message_label.pack()
            
            instruction_label = tk.Label(no_data_frame, text="Click 'Preview Data' to load preview", 
                                       font=('Arial', 10), bg='#F8F9FA', fg='#6C757D')
            instruction_label.pack(pady=(0, 20))
            return

        try:
            # Setup enhanced styling
            self.setup_preview_styles()
            
            # Use cached data for preview
            headers = self.data_cache.headers
            sample_rows = self.data_cache.sample_rows
            
            if not sample_rows:
                # Enhanced error message
                error_frame = tk.Frame(self.preview_frame, bg='#FFF5F5', relief='solid', bd=1)
                error_frame.pack(fill='both', expand=True, padx=10, pady=10)
                
                tk.Label(error_frame, text="⚠️", font=('Arial', 20), bg='#FFF5F5', fg='#E53E3E').pack(pady=(15, 5))
                tk.Label(error_frame, text="No data available for preview", 
                        font=('Arial', 11, 'bold'), bg='#FFF5F5', fg='#C53030').pack(pady=(0, 15))
                return
                
            # Calculate preview rows from sample
            total_sample = len(sample_rows)
            count = max(1, int((percentage / 100) * total_sample))
            rows = sample_rows[:count]

            # Main container with enhanced styling
            main_container = tk.Frame(self.preview_frame, bg='#FFFFFF', relief='solid', bd=1)
            main_container.pack(fill="both", expand=True, padx=8, pady=8)

            # Header section with statistics (including delimiter info)
            header_section = tk.Frame(main_container, bg='#F8F9FA', height=35)
            header_section.pack(fill='x', padx=2, pady=2)
            header_section.pack_propagate(False)
            
            # Get delimiter for display using descriptive name
            delimiter_display = self.get_delimiter_display_name(self.delimiter.get())
            
            stats_text = f"📋 {len(rows)} rows - {len(headers)} columns - Delimiter: {delimiter_display}"
            stats_label = tk.Label(header_section, text=stats_text, 
                                 font=('Arial', 9, 'bold'), bg='#F8F9FA', fg='#495057')
            stats_label.pack(side='left', padx=10, pady=8)
            
            # File type indicator
            if self.data_cache.file_info:
                file_path = self.data_cache.file_info.get('file_path', '')
                file_extension = os.path.splitext(file_path)[1].upper().lstrip('.')
                
                # Determine the display text and styling based on file type
                if self.data_cache.file_info.get('file_type') == 'json':
                    indicator_text = "📄 JSON File"
                    bg_color = '#D4EDDA'
                    fg_color = '#155724'
                elif file_extension == 'CSV':
                    indicator_text = "📊 CSV File"
                    bg_color = '#D1ECF1'
                    fg_color = '#0C5460'
                elif file_extension == 'TXT':
                    indicator_text = "📝 TXT File"
                    bg_color = '#E2E3E5'
                    fg_color = '#383D41'
                elif file_extension == 'DAT':
                    indicator_text = "💾 DAT File"
                    bg_color = '#F8D7DA'
                    fg_color = '#721C24'
                else:
                    indicator_text = f"📄 {file_extension} File" if file_extension else "📄 Data File"
                    bg_color = '#E7F3FF'
                    fg_color = '#004085'
                
                type_indicator = tk.Label(header_section, text=indicator_text, 
                                        font=('Arial', 8), bg=bg_color, fg=fg_color, 
                                        relief='solid', bd=1, padx=6, pady=2)
                type_indicator.pack(side='right', padx=10, pady=6)

            # Table container with improved scrollbars
            table_container = tk.Frame(main_container, bg='#FFFFFF')
            table_container.pack(fill="both", expand=True, padx=5, pady=5)

            # Enhanced scrollbars
            x_scroll = ttk.Scrollbar(table_container, orient="horizontal")
            y_scroll = ttk.Scrollbar(table_container, orient="vertical")

            # Main table with enhanced styling
            tree = ttk.Treeview(
                table_container,
                columns=headers,
                show='headings',
                height=6,
                style='Preview.Treeview',
                xscrollcommand=x_scroll.set,
                yscrollcommand=y_scroll.set
            )

            x_scroll.config(command=tree.xview)
            y_scroll.config(command=tree.yview)

            # Configure column headers with enhanced styling
            for i, header in enumerate(headers):
                tree.heading(header, text=f"  {header}  ", anchor='w')
                # Adjust column width based on content
                max_width = max(len(header) * 8, 100)
                if rows:
                    # Check sample data to estimate better width
                    sample_values = [str(row[i] if i < len(row) else '') for row in rows[:5]]
                    max_content = max(len(val) for val in sample_values) if sample_values else 0
                    max_width = max(max_width, min(max_content * 8, 200))
                tree.column(header, width=max_width, anchor='w', minwidth=80)

            # Add data with alternating row colors
            for i, row in enumerate(rows):
                # Ensure row has values for all columns
                padded_row = row + [''] * (len(headers) - len(row))
                values = [str(val)[:50] + '...' if len(str(val)) > 50 else str(val) for val in padded_row[:len(headers)]]
                
                # Insert with tags for alternating colors
                tags = ('evenrow',) if i % 2 == 0 else ('oddrow',)
                tree.insert("", "end", values=values, tags=tags)

            # Configure alternating row colors
            tree.tag_configure('evenrow', background='#FFFFFF')
            tree.tag_configure('oddrow', background='#F8F9FA')

            # Pack scrollbars and tree
            x_scroll.pack(side="bottom", fill="x")
            y_scroll.pack(side="right", fill="y")
            tree.pack(side="left", fill="both", expand=True)

            # Enhanced footer with detailed statistics
            footer_frame = tk.Frame(main_container, bg='#E9ECEF', height=30)
            footer_frame.pack(fill='x', padx=2, pady=2)
            footer_frame.pack_propagate(False)
            
            # Left side - preview info
            preview_info = f"Showing {len(rows):,} of {total_sample:,} sample rows ({percentage}%)"
            if self.data_cache.file_info:
                if self.data_cache.file_info.get('file_type') == 'json':
                    total_actual = self.data_cache.file_info.get('total_rows', 'Unknown')
                    preview_info += f" | Total: {total_actual:,} rows"
                elif self.data_cache.file_info.get('is_large_file'):
                    total_est = self.data_cache.file_info.get('estimated_rows', 'Unknown')
                    preview_info += f" | Est. total: {total_est:,} rows"
            
            info_label = tk.Label(footer_frame, text=preview_info, 
                                font=('Arial', 8), bg='#E9ECEF', fg='#495057')
            info_label.pack(side='left', padx=8, pady=6)
            
            # Right side - data quality indicator
            quality_text = "✓ Data loaded successfully"
            quality_label = tk.Label(footer_frame, text=quality_text, 
                                   font=('Arial', 8), bg='#D4EDDA', fg='#155724',
                                   relief='solid', bd=1, padx=4, pady=2)
            quality_label.pack(side='right', padx=8, pady=4)

        except Exception as e:
            # Enhanced error display
            error_container = tk.Frame(self.preview_frame, bg='#F8D7DA', relief='solid', bd=1)
            error_container.pack(fill='both', expand=True, padx=10, pady=10)
            
            tk.Label(error_container, text="❌", font=('Arial', 20), bg='#F8D7DA', fg='#721C24').pack(pady=(15, 5))
            tk.Label(error_container, text="Preview Error", 
                    font=('Arial', 12, 'bold'), bg='#F8D7DA', fg='#721C24').pack()
            tk.Label(error_container, text=str(e), 
                    font=('Arial', 9), bg='#F8D7DA', fg='#721C24', wraplength=400).pack(pady=(5, 15))

    def apply_config_settings(self):
        self.config_mgr.load()
        cfg = self.config_mgr.config
        self.schema_name.set(cfg.get("default_schema", "dbo"))
        self.database_name.set(cfg.get("default_database", ""))
        self.preview_percentage_var.set(str(cfg.get("default_preview_percentage", 5)))
        self.include_create_script.set(cfg.get("default_include_create", True))
        self.include_insert_script.set(cfg.get("default_include_insert", True))
        self.infer_types_var.set(cfg.get("default_infer_types", True))
        self.sample_percentage = cfg.get("sample_percentage", 15)

        self.insert_batch_size = int(cfg.get("insert_batch_size") or 500)
         # Only initialize truncate_before_insert if it doesn't exist, then apply config setting
        if not hasattr(self, 'truncate_before_insert'):
            self.truncate_before_insert = tk.BooleanVar()
        self.truncate_before_insert.set(cfg.get("default_truncate", False))
        self.batch_insert_var.set(cfg.get("default_batch_insert", True))
        self.max_additional_columns = int(cfg.get("max_additional_columns", 1))

        # Load new table name configuration settings
        self.use_filename_as_table_name = cfg.get("use_filename_as_table_name", True)
        self.custom_table_name = cfg.get("custom_table_name", "")
        
        # NEW: Load auto preview data setting
        self.auto_preview_data = cfg.get("auto_preview_data", True)
        
        # NEW: Load default column format setting  
        self.default_column_format = cfg.get("default_column_format", "Source File")
        
        # NEW: Load large file threshold setting
        self.large_file_threshold_mb = cfg.get("large_file_threshold_mb", 1000)
        
        # Update large file indicator if a file is currently selected
        current_file = self.file_path.get()
        if current_file:
            self.update_large_file_indicator(current_file)

        # Update truncate color if the checkbox widget exists
        try:
            if hasattr(self, 'truncate_check') and self.truncate_check.winfo_exists():
                self.update_truncate_color()
        except Exception:
            pass

        # Try to update batch checkbox label if present
        try:
            for widget in self.master.winfo_children():
                if isinstance(widget, tk.LabelFrame) and 'Script Generator' in widget.cget('text'):
                    for cb in widget.winfo_children():
                        for child in cb.winfo_children():
                            if isinstance(child, tk.Checkbutton) and 'Batch' in child.cget('text'):
                                child.config(text=f"Batch ({self.insert_batch_size})")
        except Exception:
            pass

    def open_github_repository(self):
        """Open the GitHub repository in the default web browser"""
        import webbrowser
        try:
            webbrowser.open("https://github.com/jackworthen/sql-builder")
        except Exception as e:
            messagebox.showerror("Error", f"Could not open GitHub repository: {e}")

    def add_new_column_row(self):
        if self.additional_column_count >= self.max_additional_columns:
            return
            
        row = tk.Frame(self.scrollable_frame)
        row.pack(fill="x", pady=1, anchor="w")

        pk_var = tk.BooleanVar()
        null_var = tk.BooleanVar(value=True)

        name_entry = tk.Entry(row, width=30)
        # Get the correct index for the new column (append at end)
        new_index = len(self.pk_vars)
        pk_checkbox = tk.Checkbutton(row, variable=pk_var, command=lambda idx=new_index: self.update_pk_states(idx))
        pk_checkbox.pack(side="left")

        name_entry.pack(side="left")

        type_combo = ttk.Combobox(row, width=27, values=self.sql_data_types)
        type_combo.pack(side="left", padx=5)
        
        # Simple event bindings that actually work
        type_combo.bind("<<ComboboxSelected>>", lambda e, idx=new_index: self.enable_reset_button(idx))
        type_combo.bind("<KeyRelease>", lambda e, idx=new_index: self.enable_reset_button(idx))
        type_combo.bind("<Button-1>", lambda e, idx=new_index: self.enable_reset_button(idx))

        null_checkbox = tk.Checkbutton(row, variable=null_var, command=lambda idx=new_index: self.update_null_states(idx))
        null_checkbox.pack(side="left")

        # Append new column at the end instead of inserting at beginning
        self.pk_vars.append(pk_var)
        self.null_vars.append(null_var)
        self.pk_checkboxes.append(pk_checkbox)
        self.null_checkboxes.append(null_checkbox)
        self.column_entries.append(name_entry)
        self.type_entries.append(type_combo)

        self.additional_column_count += 1
        if self.additional_column_count >= self.max_additional_columns:
            self.add_column_button.config(state="disabled")
        
        # Enable the remove column button since we now have at least one added column
        self.remove_column_button.config(state="normal")

    def remove_last_column(self):
        """Remove the last added column"""
        if self.additional_column_count <= 0:
            return
        
        # Calculate the index of the last added column
        original_column_count = len(self.headers)
        last_column_index = len(self.column_entries) - 1
        
        # Only remove if the last column is an added one (not original)
        if last_column_index >= original_column_count:
            # Get the UI row widget to destroy it
            # The row widget is the parent of the entry widget
            row_widget = self.column_entries[last_column_index].master
            
            # Remove from all lists (last element)
            self.column_entries.pop()
            self.type_entries.pop()
            self.pk_vars.pop()
            self.pk_checkboxes.pop()
            self.null_vars.pop()
            self.null_checkboxes.pop()
            
            # Destroy the UI row
            row_widget.destroy()
            
            # Update counters
            self.additional_column_count -= 1
            
            # Update button states
            if self.additional_column_count <= 0:
                # No more added columns, disable remove button
                self.remove_column_button.config(state="disabled")
            
            # Re-enable add button if it was disabled due to max columns
            if self.additional_column_count < self.max_additional_columns:
                self.add_column_button.config(state="normal")

    def safe_exit(self):
        """Safely exit the application by shutting down background threads"""
        try:
            # Shutdown the executor and wait briefly for threads to complete
            self.executor.shutdown(wait=False)
            # Force quit immediately without waiting for all threads
            self.master.quit()
            self.master.destroy()
        except Exception:
            # If anything goes wrong, force quit anyway
            self.master.quit()

    def __del__(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
        except Exception:
            pass

if __name__ == "__main__":
    root = tk.Tk()
    app = SQLTableBuilder(root)
    root.mainloop()