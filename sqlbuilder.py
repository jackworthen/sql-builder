from config_manager import ConfigManager
import re
from datetime import datetime
from collections import defaultdict, Counter
import threading
from concurrent.futures import ThreadPoolExecutor
import time

def resource_path(filename):
    """ Get absolute path to resource, works for dev and for PyInstaller bundle """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.abspath("."), filename)

import tkinter as tk
from tkinter import ttk
from tkinter import filedialog, messagebox
import csv
import sys
import os

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
        
    def clear(self):
        """Clear cached data"""
        self.headers = None
        self.sample_rows = None
        self.all_rows = None
        self.file_info = None
        self.is_loaded = False
        self.is_large_file = False
        self.chunk_generator = None
        
    def load_file(self, file_path, delimiter, sample_percentage=15, large_file_threshold=50000):
        """Load file with smart caching strategy"""
        self.clear()
        
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
        
        if self.is_large_file:
            self._load_large_file(file_path, delimiter, sample_percentage)
        else:
            self._load_small_file(file_path, delimiter, sample_percentage)
            
        self.file_info = {
            'total_rows': len(self.all_rows) if self.all_rows else estimated_rows,
            'delimiter': delimiter,
            'file_path': file_path,
            'is_large_file': self.is_large_file,
            'estimated_rows': estimated_rows
        }
        self.is_loaded = True
        
    def _load_small_file(self, file_path, delimiter, sample_percentage):
        """Load entire file for small datasets"""
        with open(file_path, 'r', newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            self.headers = next(reader)
            self.all_rows = list(reader)
            
        # Create sample for type inference
        sample_size = max(100, int(len(self.all_rows) * sample_percentage / 100))
        self.sample_rows = self.all_rows[:sample_size]
        
    def _load_large_file(self, file_path, delimiter, sample_percentage, chunk_size=10000):
        """Load only sample for large files"""
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
        if not self.is_large_file and self.all_rows:
            # For small files, chunk the loaded data
            for i in range(0, len(self.all_rows), chunk_size):
                yield self.all_rows[i:i + chunk_size]
        else:
            # For large files, read chunks from file
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
        
        self.label = tk.Label(self.window, text="Initializing...")
        self.label.pack(pady=20)
        
        self.progress = ttk.Progressbar(self.window, mode='indeterminate')
        self.progress.pack(pady=10, padx=40, fill='x')
        self.progress.start()
        
        # Create style for this button
        style = ttk.Style()
        style.configure('ProgressCancel.TButton', 
                       background='#FFB6C1',  # Light pink
                       foreground='black',
                       padding=(5, 2),        # Smaller padding
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
        self.column_entries = []
        self.type_entries = []
        self.pk_vars = []
        self.pk_checkboxes = []
        self.null_vars = []
        self.null_checkboxes = []
        self.use_identity = tk.BooleanVar()
        self.use_guid = tk.BooleanVar()
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
        self.identity_guid_enabled = False
        self.insert_batch_size = int(cfg.get("insert_batch_size") or 500)
        self.truncate_before_insert = tk.BooleanVar()
        self.infer_types_var = tk.BooleanVar(value=cfg["default_infer_types"])
        self.sample_percentage = cfg["sample_percentage"]

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
  
    def build_file_selection_screen(self):
        self.master.iconbitmap(resource_path('sqlbuilder_icon.ico'))  # This sets the icon
        self.use_identity.set(False)
        self.use_guid.set(False)
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
                
        # Action button
        self.preview_frame = tk.LabelFrame(main_frame, text="Data Preview", padx=5, pady=5)
        self.preview_frame.pack(fill="both", expand=True, pady=(10, 10))

        action_frame = tk.Frame(main_frame)
        action_frame.pack(pady=15, fill="x")
        self.next_button = ttk.Button(action_frame, text="Next →", 
                                     style='Primary.TButton',
                                     width=8, state="disabled", 
                                     command=self.process_file)
        self.next_button.pack(side="left")
        
        # Add Clear and Exit buttons on the right side
        ttk.Button(action_frame, text="Exit", 
                  style='Secondary.TButton',
                  width=8, 
                  command=self.safe_exit).pack(side="right")
        
        ttk.Button(action_frame, text="Clear", 
                  style='Secondary.TButton',
                  width=8, 
                  command=self.clear_data).pack(side="right", padx=(0, 10))
                
    def browse_file(self):
        filetypes = [("Data Files", "*.csv *.txt *.dat"), ("All Files", "*.*")]
        selected_path = filedialog.askopenfilename(title="Open File", filetypes=filetypes)
        if selected_path:
            self.file_path.set(selected_path)
            self.infer_delimiter()
            default_name = os.path.splitext(os.path.basename(selected_path))[0]
            self.table_name.set(default_name)
            
            # Clear any existing cached data when new file is selected
            self.data_cache.clear()
            
            self.next_button.config(state="normal")
            self.show_button.config(state="normal")
            
            # Clear any existing preview
            for widget in self.preview_frame.winfo_children():
                widget.destroy()

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
        
        # Clear preview display
        for widget in self.preview_frame.winfo_children():
            widget.destroy()
        
        # Disable buttons until new file is selected
        self.show_button.config(state="disabled")
        self.next_button.config(state="disabled")

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
                delimiter_val = "\t" if delimiter_val == "\\t" else delimiter_val
                
                # Load file into cache
                progress.update_text("Loading data into cache...")
                self.data_cache.load_file(path, delimiter_val, self.sample_percentage)
                
                if progress.cancelled:
                    return
                    
                self.headers = self.data_cache.headers
                
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
        self.master.geometry("530x800")
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

        # Options
        options_frame = tk.Frame(settings_frame)
        options_frame.pack(fill="x", pady=5)
        self.identity_checkbox = tk.Checkbutton(
            options_frame, text="INT IDENTITY", variable=self.use_identity, command=self.update_identity_guid_states
        )
        self.identity_checkbox.pack(side="left", padx=5)

        self.guid_checkbox = tk.Checkbutton(
            options_frame, text="UNIQUEIDENTIFIER", variable=self.use_guid, command=self.update_identity_guid_states
        )
        self.guid_checkbox.pack(side="left", padx=5)
        
        # Row for renaming dropdown and set button
        rename_frame = tk.Frame(settings_frame)
        rename_frame.pack(fill="x", pady=3)
        tk.Label(rename_frame, text="Format Columns:").pack(side="left", padx=(5, 2))
        self.naming_style_var = tk.StringVar(value="")
        self.naming_combo = ttk.Combobox(rename_frame, textvariable=self.naming_style_var, values=["CamelCase", "snake_case", "lowercase", "UPPERCASE"], width=15, state="readonly")
        self.naming_combo.pack(side="left", padx=2)
        ttk.Button(rename_frame, text="Set", 
                  style='Small.TButton',
                  width=4, 
                  command=self.apply_column_naming_convention).pack(side="left", padx=5)
        
        self.reset_button = ttk.Button(rename_frame, text="Reset Types", 
                                      style='Small.TButton',
                                      command=self.reset_data_types_immediately, 
                                      state="disabled")
        self.reset_button.pack(side="left", padx=5)    
        
        # Disable checkboxes initially
        self.identity_checkbox.config(state="disabled")
        self.guid_checkbox.config(state="disabled")
        self.identity_guid_enabled = False

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
            type_combo.bind("<<ComboboxSelected>>", lambda e: self.enable_reset_button())
            type_combo.bind("<KeyRelease>", lambda e: self.enable_reset_button())
            type_combo.bind("<Button-1>", lambda e: self.enable_reset_button())

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

        # === SCRIPT GENERATOR SECTION ===
        script_frame = tk.LabelFrame(self.master, text="Script Generator", padx=10, pady=10)
        script_frame.pack(fill="x", padx=10, pady=(10, 0))
        
        checkbox_row = tk.Frame(script_frame)
        checkbox_row.pack(fill="x", pady=5)

        truncate_row = tk.Frame(script_frame)
        truncate_row.pack(fill="x", pady=2)
        self.truncate_check = tk.Checkbutton(truncate_row, text="TRUNCATE", variable=self.truncate_before_insert, command=self.update_truncate_color)
        self.truncate_check.pack(side="left", padx=10)
        tk.Checkbutton(checkbox_row, text="CREATE TABLE", variable=self.include_create_script).pack(side="left", padx=10)
        insert_checkbox = tk.Checkbutton(checkbox_row, text="INSERT INTO", variable=self.include_insert_script)
        insert_checkbox.pack(side="left", padx=10)
        self.include_insert_script.trace_add("write", self.update_truncate_enable_state)
        self.batch_insert_check = tk.Checkbutton(checkbox_row, text=f"Batch INSERT ({self.insert_batch_size})", variable=self.batch_insert_var)
        self.batch_insert_check.pack(side="left", padx=5)
        ttk.Button(checkbox_row, text="Save", 
                  style='Success.TButton',
                  width=9, 
                  command=self.handle_generate_scripts).pack(side="right", padx=10)
        
        # File info display (rows count) - positioned beneath Batch INSERT
        if self.data_cache.is_loaded:
            info_frame = tk.Frame(script_frame)
            info_frame.pack(fill="x", pady=(5, 0))
            file_info = self.data_cache.file_info
            rows_text = f"~{file_info['estimated_rows']:,}" if file_info['is_large_file'] else f"{file_info['total_rows']:,}"
            filename = os.path.basename(self.file_path.get())
            tk.Label(info_frame, text=f"Total Rows in {filename}: {rows_text}", fg="blue", font=('Arial', 9)).pack(side="left", padx=10)
        
        self.update_truncate_enable_state()

        # === BACK AND EXIT BUTTONS ===

        back_frame = tk.Frame(self.master)
        back_frame.pack(pady=(5, 15))
        ttk.Button(back_frame, text="← Back", 
                  style='Secondary.TButton',
                  width=8, 
                  command=self.build_file_selection_screen).pack(side="left", padx=10)
        ttk.Button(back_frame, text="Exit", 
                  style='Secondary.TButton',
                  width=8, 
                  command=self.safe_exit).pack(side="left", padx=10)

    def handle_generate_scripts(self):
        if self.include_create_script.get():
            self.generate_sql_file()
        if self.include_insert_script.get():
            self.generate_insert_statements_optimized()
    
    def update_identity_guid_states(self):
        if self.use_identity.get():
            self.guid_checkbox.config(state="disabled")
        else:
            self.guid_checkbox.config(state="normal")

        if self.use_guid.get():
            self.identity_checkbox.config(state="disabled")
        else:
            self.identity_checkbox.config(state="normal")

        for idx, pk_var in enumerate(self.pk_vars):
            if pk_var.get():
                current_entry = self.type_entries[idx]
                current_val = current_entry.get().strip().upper()

                if self.use_identity.get():
                    if current_val != "INT IDENTITY":
                        current_entry.delete(0, tk.END)
                        current_entry.insert(0, "INT IDENTITY")
                elif self.use_guid.get():
                    if current_val != "UNIQUEIDENTIFIER":
                        current_entry.delete(0, tk.END)
                        current_entry.insert(0, "UNIQUEIDENTIFIER")
                else:
                    # Neither identity nor guid selected, and current type matches either: clear it
                    if current_val in ["INT IDENTITY", "UNIQUEIDENTIFIER"]:
                        current_entry.delete(0, tk.END)

    def enable_reset_button(self):
        """Enable the reset button"""
        if hasattr(self, 'reset_button') and self.reset_button.winfo_exists():
            self.reset_button.config(state="normal")

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

    def update_pk_states(self, selected_index):
        for i, (pk_var, pk_checkbox, null_var, null_checkbox) in enumerate(
            zip(self.pk_vars, self.pk_checkboxes, self.null_vars, self.null_checkboxes)
        ):
            if self.pk_vars[selected_index].get():
                if i != selected_index:
                    pk_var.set(False)
                    pk_checkbox.config(state="disabled")
                else:
                    pk_checkbox.config(state="normal")
                    null_var.set(False)
                    null_checkbox.config(state="disabled")

                    if self.use_identity.get():
                        new_type = "INT IDENTITY"
                    elif self.use_guid.get():
                        new_type = "UNIQUEIDENTIFIER"
                    else:
                        new_type = None

                    if new_type:
                        self.type_entries[selected_index].delete(0, tk.END)
                        self.type_entries[selected_index].insert(0, new_type)
            else:
                pk_checkbox.config(state="normal")
                null_checkbox.config(state="normal")

    def update_null_states(self, selected_index):
        for i, (pk_var, pk_checkbox, null_var, null_checkbox) in enumerate(
            zip(self.pk_vars, self.pk_checkboxes, self.null_vars, self.null_checkboxes)
        ):
            if null_var.get():
                pk_var.set(False)
                pk_checkbox.config(state="disabled")
            else:
                pk_checkbox.config(state="normal")

    def generate_sql_file(self):
        table_name = self.table_name.get().strip()
        schema_name = self.schema_name.get().strip()
        full_table = f"[{schema_name}].[{table_name}]"
        if not table_name:
            return

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

    def generate_insert_statements_optimized(self):
        """Optimized insert statement generation with chunked processing"""
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

        # Show progress dialog for large files
        progress = None
        if self.data_cache.file_info['is_large_file']:
            progress = ProgressWindow(self.master, "Generating INSERT Statements...")
            
        def generate_task():
            try:
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
                
                if progress:
                    progress.update_text("Processing data chunks...")
                
                # Process data in chunks
                all_values = []
                chunk_count = 0
                
                for chunk in self.data_cache.get_chunk_generator():
                    if progress and progress.cancelled:
                        return
                        
                    chunk_count += 1
                    if progress:
                        progress.update_text(f"Processing chunk {chunk_count}...")
                    
                    for row in chunk:
                        # Pad row if necessary
                        extra_count = len(column_types) - len(row)
                        if extra_count > 0:
                            row = [''] * extra_count + row
                        all_values.append(self.format_insert_values(row, column_types))
                
                if progress and progress.cancelled:
                    return
                    
                # Generate final script
                if progress:
                    progress.update_text("Generating final script...")
                
                if self.batch_insert_var.get():
                    for i in range(0, len(all_values), self.insert_batch_size):
                        chunk = all_values[i:i + self.insert_batch_size]
                        script_lines.append(insert_header)
                        script_lines.append(",\n".join(chunk) + ";\nGO")
                else:
                    script_lines.append(insert_header)
                    script_lines.append(",\n".join(all_values) + ";\nGO")

                script = "\n".join(script_lines)
                
                if progress:
                    progress.update_text("Saving file...")
                
                with open(file_path, 'w') as f:
                    f.write(script)
                
                # Schedule UI update on main thread
                if progress:
                    self.master.after(0, lambda: [progress.close(), messagebox.showinfo("Success", f"INSERT statements saved to {file_path}")])
                else:
                    self.master.after(0, lambda: messagebox.showinfo("Success", f"INSERT statements saved to {file_path}"))
                    
            except Exception as e:
                if progress:
                    self.master.after(0, lambda: [progress.close(), messagebox.showerror("Error", f"Failed to generate INSERT statements: {e}")])
                else:
                    self.master.after(0, lambda: messagebox.showerror("Error", f"Failed to generate INSERT statements: {e}"))

        if progress:
            # Run generation in background thread for large files
            self.executor.submit(generate_task)
        else:
            # Run directly for small files
            generate_task()

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
            
            # Clear added columns' data types if neither INT IDENTITY nor UNIQUEIDENTIFIER is checked
            if not self.use_identity.get() and not self.use_guid.get():
                for i in range(original_column_count, len(self.type_entries)):
                    combo = self.type_entries[i]
                    combo.delete(0, "end")
            
            # Keep reset button enabled for future use
            self.reset_button.config(state="normal")
                
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
                        
                        # Clear added columns' data types if neither INT IDENTITY nor UNIQUEIDENTIFIER is checked
                        if not self.use_identity.get() and not self.use_guid.get():
                            for i in range(original_column_count, len(self.type_entries)):
                                combo = self.type_entries[i]
                                combo.delete(0, "end")
                        
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
                delimiter_val = "\t" if delimiter_val == "\\t" else delimiter_val
                
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
            if self.data_cache.file_info and self.data_cache.file_info.get('is_large_file'):
                type_indicator = tk.Label(header_section, text="🔍 Large File Mode", 
                                        font=('Arial', 8), bg='#FFF3CD', fg='#856404', 
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
            if self.data_cache.file_info and self.data_cache.file_info.get('is_large_file'):
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
        self.truncate_before_insert = tk.BooleanVar()
        self.batch_insert_var.set(cfg.get("default_batch_insert", True))
        self.max_additional_columns = int(cfg.get("max_additional_columns", 1))

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
        # Enable the identity/guid checkboxes if not already
        if not self.identity_guid_enabled:
            self.identity_checkbox.config(state="normal")
            self.guid_checkbox.config(state="normal")
            self.identity_guid_enabled = True
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
        type_combo.bind("<<ComboboxSelected>>", lambda e: self.enable_reset_button())
        type_combo.bind("<KeyRelease>", lambda e: self.enable_reset_button())
        type_combo.bind("<Button-1>", lambda e: self.enable_reset_button())

        null_checkbox = tk.Checkbutton(row, variable=null_var, command=lambda idx=new_index: self.update_null_states(idx))
        null_checkbox.pack(side="left")
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
                # Also disable identity/guid checkboxes if no added columns
                if not self.identity_guid_enabled:
                    self.identity_checkbox.config(state="disabled")
                    self.guid_checkbox.config(state="disabled")
                    self.identity_guid_enabled = False
            
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