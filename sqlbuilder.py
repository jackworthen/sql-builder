from config_manager import ConfigManager
from sql_engine import DataCache, OptimizedTypeInferrer, SQLGenerator
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
import logging

def resource_path(filename):
    """ Get absolute path to resource, works for dev and for PyInstaller bundle """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.abspath("."), filename)

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
        
        # Initialize optimized components from engine
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
        self.master.geometry("650x800")
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

        # === COLUMN DEFINITION SECTION ===
        column_frame = tk.LabelFrame(self.master, text="Define Table Columns", padx=10, pady=10)
        column_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Add Column and Remove Column buttons at the top of the frame
        column_buttons_frame = tk.Frame(column_frame)
        column_buttons_frame.pack(fill="x", pady=(0, 10))
        
        # Column Format dropdown - moved to the left of Add Column button
        tk.Label(column_buttons_frame, text="Column Format:").pack(side="left", padx=(0, 5))
        self.naming_style_var = tk.StringVar(value=self.config_mgr.config.get("default_column_format", "Source File"))
        self.naming_combo = ttk.Combobox(column_buttons_frame, textvariable=self.naming_style_var, 
                                        values=["Source File", "CamelCase", "snake_case", "lowercase", "UPPERCASE"], 
                                        width=12, state="readonly")
        self.naming_combo.pack(side="left", padx=(2, 15))
        
        # Bind the dropdown selection to automatically apply the formatting
        self.naming_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_column_naming_convention())
        
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
        tk.Label(header_row, text="🔑", width=4, anchor="w").pack(side="left")
        tk.Label(header_row, text="Column Name", width=30, anchor="w").pack(side="left")
        tk.Label(header_row, text="Data Type", width=30, anchor="w").pack(side="left")
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
        # Validate table name before proceeding
        table_name = self.table_name.get().strip()
        if not table_name:
            messagebox.showerror("Missing Table Name", 
                               "Please enter a table name before saving the scripts.\n\n"
                               "The table name is required to generate valid SQL scripts.")
            return
        
        # Start timing for logging
        start_time = time.time()
        
        # Initialize log data
        log_data = {
            'operation_successful': True,
            'source_file_path': self.file_path.get(),
            'source_file_name': os.path.basename(self.file_path.get()) if self.file_path.get() else 'N/A',
            'source_file_type': self.data_cache.file_type.upper() if self.data_cache.file_type else 'N/A',
            'source_total_rows': self.data_cache.file_info.get('total_rows', 0) if self.data_cache.file_info else 0,
            'source_delimiter': self.delimiter.get() if self.delimiter.get() != "N/A (JSON)" else None,
            'database_name': self.database_name.get().strip() or 'N/A',
            'schema_name': self.schema_name.get().strip() or 'dbo',
            'table_name': table_name,
            'full_table_name': f"[{self.schema_name.get().strip() or 'dbo'}].[{table_name}]",
            'column_count': len(self.column_entries),
            'type_inference_enabled': self.infer_types_var.get(),
            'column_format': self.naming_style_var.get() if hasattr(self, 'naming_style_var') else 'Source File',
            'batch_insert_enabled': self.batch_insert_var.get(),
            'batch_size': self.insert_batch_size,
            'truncate_enabled': self.truncate_before_insert.get(),
            'create_script_generated': False,
            'insert_script_generated': False,
            'insert_rows_processed': 0,
            'start_time': start_time
        }
        
        # Add source file size
        if self.file_path.get():
            try:
                file_size = os.path.getsize(self.file_path.get())
                if file_size < 1024:
                    log_data['source_file_size'] = f"{file_size} bytes"
                elif file_size < 1024 * 1024:
                    log_data['source_file_size'] = f"{file_size / 1024:.1f} KB"
                else:
                    log_data['source_file_size'] = f"{file_size / (1024 * 1024):.1f} MB"
            except Exception:
                log_data['source_file_size'] = 'Unknown'
        
        # Collect column details
        column_details = []
        primary_key_columns = []
        
        for i, (col_entry, type_entry, pk_var, null_var) in enumerate(zip(
            self.column_entries, self.type_entries, self.pk_vars, self.null_vars)):
            col_name = col_entry.get()
            col_type = type_entry.get()
            is_pk = pk_var.get()
            allows_null = null_var.get()
            
            if is_pk:
                primary_key_columns.append(col_name)
            
            column_details.append({
                'name': col_name,
                'type': col_type,
                'is_primary_key': is_pk,
                'allows_null': allows_null
            })
        
        log_data['column_details'] = column_details
        log_data['primary_key_columns'] = primary_key_columns
        
        create_file_path = None
        
        try:
            # Generate CREATE script
            if self.include_create_script.get():
                create_file_path = self.generate_sql_file()
                if create_file_path:
                    log_data['create_script_generated'] = True
                    log_data['create_script_name'] = os.path.basename(create_file_path)
                    log_data['create_script_path'] = create_file_path
                    try:
                        script_size = os.path.getsize(create_file_path)
                        if script_size < 1024:
                            log_data['create_script_size'] = f"{script_size} bytes"
                        else:
                            log_data['create_script_size'] = f"{script_size / 1024:.1f} KB"
                    except Exception:
                        log_data['create_script_size'] = 'Unknown'
            
            # Generate INSERT script
            if self.include_insert_script.get():
                # Pass a callback to write the log after INSERT completion
                self.generate_insert_statements_optimized(create_file_path, log_data, self.finalize_operation_log)
            else:
                # Write log immediately if no INSERT script
                self.finalize_operation_log(log_data)
                
        except Exception as e:
            log_data['operation_successful'] = False
            log_data['notes'] = f"Error during script generation: {str(e)}"
            self.finalize_operation_log(log_data)
    
    def finalize_operation_log(self, log_data):
        """Finalize and write the operation log"""
        # Calculate total processing time
        end_time = time.time()
        processing_time = end_time - log_data['start_time']
        if processing_time < 60:
            log_data['total_processing_time'] = f"{processing_time:.1f} seconds"
        else:
            minutes = int(processing_time // 60)
            seconds = processing_time % 60
            log_data['total_processing_time'] = f"{minutes}m {seconds:.1f}s"
        
        # Remove start_time from log_data as it's no longer needed
        log_data.pop('start_time', None)
        
        # Write operation log
        self.write_operation_log(log_data)

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
        if not table_name:
            return None

        db_name = self.database_name.get().strip()
        
        columns = []
        for i, (col_entry, type_entry, pk_var, null_var) in enumerate(zip(self.column_entries, self.type_entries, self.pk_vars, self.null_vars)):
            columns.append({
                'name': col_entry.get(),
                'type': type_entry.get(),
                'is_pk': pk_var.get(),
                'allows_null': null_var.get()
            })

        script = SQLGenerator.generate_create_table_script(db_name, schema_name, table_name, columns)

        default_filename = f"create_table_{table_name}.sql"
        file_path = filedialog.asksaveasfilename(defaultextension=".sql", initialfile=default_filename, filetypes=[("SQL Files", "*.sql")])
        if file_path:
            with open(file_path, 'w') as f:
                f.write(script)
            return file_path
        return None

    def generate_insert_statements_optimized(self, create_file_path=None, log_data=None, log_callback=None):
        """Optimized insert statement generation with chunked processing and progress tracking"""
        table_name = self.table_name.get().strip()
        schema_name = self.schema_name.get().strip()
        if not table_name:
            return None
        
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
            # Call log callback even if user cancels
            if log_callback and log_data:
                log_callback(log_data)
            return None

        # Always show progress dialog for INSERT generation
        progress = ProgressWindow(self.master, "Generating INSERT Statements...")
        
        def generate_task():
            try:
                db_name = self.database_name.get().strip()
                
                def progress_cb(text):
                    self.master.after(0, lambda: progress.update_text(text))
                
                def cancel_check():
                    return progress.cancelled

                total_rows_processed = SQLGenerator.generate_insert_script(
                    file_path, table_name, schema_name, db_name, col_names, column_types,
                    self.data_cache, self.batch_insert_var.get(), self.insert_batch_size,
                    self.truncate_before_insert.get(), progress_cb, cancel_check
                )
                
                if total_rows_processed is None: # Cancelled
                    if log_callback and log_data:
                        log_data['operation_successful'] = False
                        log_data['notes'] = "Operation cancelled by user"
                        self.master.after(0, lambda: log_callback(log_data))
                    return

                # Update log data if provided
                if log_data is not None:
                    log_data['insert_script_generated'] = True
                    log_data['insert_script_name'] = os.path.basename(file_path)
                    log_data['insert_script_path'] = file_path
                    log_data['insert_rows_processed'] = total_rows_processed
                    
                    # Calculate file size
                    try:
                        script_size = os.path.getsize(file_path)
                        if script_size < 1024 * 1024:
                            log_data['insert_script_size'] = f"{script_size / 1024:.1f} KB"
                        else:
                            log_data['insert_script_size'] = f"{script_size / (1024 * 1024):.1f} MB"
                    except Exception:
                        log_data['insert_script_size'] = 'Unknown'
                
                # Build completion message
                if create_file_path and log_data and log_data.get('insert_script_generated'):
                    completion_msg = "✅ Operation completed successfully!\n\nCREATE TABLE and INSERT scripts have been generated."
                elif create_file_path:
                    completion_msg = "✅ Operation completed successfully!\n\nCREATE TABLE script has been generated."
                else:
                    completion_msg = "✅ Operation completed successfully!\n\nINSERT script has been generated."
                
                # Schedule UI update and log callback on main thread
                def complete_operation():
                    progress.update_text(completion_msg)
                    progress.show_completion(dual_scripts=bool(create_file_path))
                    # Call log callback after INSERT is complete
                    if log_callback and log_data:
                        log_callback(log_data)
                
                self.master.after(0, complete_operation)
                
                return file_path
                    
            except Exception as e:
                error_msg = f"❌ Operation failed!\n\n{str(e)}"
                
                # Update log data with error info
                if log_data is not None:
                    log_data['operation_successful'] = False
                    log_data['notes'] = f"Error during INSERT generation: {str(e)}"
                
                def handle_error():
                    progress.update_text(error_msg)
                    progress.show_completion(dual_scripts=False)
                    # Call log callback even on error
                    if log_callback and log_data:
                        log_callback(log_data)
                
                self.master.after(0, handle_error)
                return None

        # Always run generation in background thread
        return self.executor.submit(generate_task)

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
        return SQLGenerator.format_column_name(name, style)

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
        
        # NEW: Load logging settings
        self.enable_logging = cfg.get("enable_logging", True)
        self.log_directory = cfg.get("log_directory", "")
        
        # Update large file indicator if a file is currently selected
        current_file = self.file_path.get()
        if current_file:
            self.update_large_file_indicator(current_file)
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

    def write_operation_log(self, log_data):
        """Write comprehensive operation log using the SQLGenerator engine"""
        SQLGenerator.write_operation_log(log_data, self.enable_logging, self.log_directory)

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
