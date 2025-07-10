# config_manager.py
import json
import sys
import os
import tkinter as tk
from tkinter import messagebox, ttk, filedialog

def resource_path(filename):
    """ Get absolute path to resource, works for dev and for PyInstaller bundle """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.abspath("."), filename)

def get_config_path():
    import platform
    import os

    app_name = "SQLTableBuilderPro"

    if platform.system() == "Windows":
        base_dir = os.environ.get("APPDATA", os.path.expanduser("~"))
    else:
        base_dir = os.path.join(os.path.expanduser("~"), ".config")

    config_dir = os.path.join(base_dir, app_name)
    os.makedirs(config_dir, exist_ok=True)
    return os.path.join(config_dir, "config.json")

CONFIG_FILE = get_config_path()

class ConfigManager:
    def __init__(self, path=CONFIG_FILE):
        self.path = path
        self.config = {
            "default_database": "",
            "default_schema": "dbo",
            "max_additional_columns": 1,
            "default_preview_percentage": 10,
            "sample_percentage": 15,
            "default_infer_types": True,
            "default_include_create": True,
            "default_include_insert": True,
            "default_batch_insert": False,
            "default_truncate": False,
            "insert_batch_size": 5000,
            "use_filename_as_table_name": True,
            "custom_table_name": "",
            "auto_preview_data": True,
            "default_column_format": "Source File",
            "large_file_threshold_mb": 1000,
            "enable_logging": True,
            "log_directory": ""
        }
        self.load()

    def load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, 'r') as f:
                    self.config.update(json.load(f))
            except Exception as e:
                print(f"Error loading config: {e}")

    def save(self):
        try:
            with open(self.path, 'w') as f:
                json.dump(self.config, f, indent=4)
        except Exception as e:
            print(f"Error saving config: {e}")

    def setup_button_styles(self):
        """Configure button styles with compact design and light blue theme"""
        style = ttk.Style()
        
        # Configure light blue button style - smaller with black text on light blue
        style.configure('LightBlue.TButton',
                       background='#ADD8E6',  # Light blue
                       foreground='black',
                       padding=(6, 3),        # Reduced from (15, 8)
                       relief='flat',         # Changed from raised to flat
                       borderwidth=1,
                       focuscolor='none',
                       font=('Arial', 8))
        
        # Configure hover and pressed effects
        style.map('LightBlue.TButton',
                 background=[('active', '#87CEEB'),   # Slightly darker light blue
                            ('pressed', '#87CEFA')],  # Sky blue
                 relief=[('pressed', 'flat'),
                        ('!pressed', 'flat')])

    def open_settings_window(self, master, on_save_callback=None):
        window = tk.Toplevel(master)
        window.iconbitmap(resource_path('sqlbuilder_icon.ico'))
        window.title("Settings")
        window.geometry("420x525")  # Increased width to show Browse button, reduced height after removing Log Content
        window.resizable(False, False)
        
        # Center the window
        window.transient(master)
        window.grab_set()
        
        # Configure window style
        window.configure(bg='#f0f0f0')
        
        # Setup button styling
        self.setup_button_styles()
        
        # Create main frame with padding
        main_frame = ttk.Frame(window, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create notebook for tabs
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Dictionary to store all entry widgets
        entries = {}
        
        # Configuration Tab (moved to first position)
        db_frame = ttk.Frame(notebook, padding="20")
        notebook.add(db_frame, text="Configuration")
        
        self._create_database_tab(db_frame, entries)
        
        # Data Processing Tab  
        processing_frame = ttk.Frame(notebook, padding="20")
        notebook.add(processing_frame, text="Data Processing")
        
        self._create_processing_tab(processing_frame, entries)
        
        # SQL Generation Tab
        sql_frame = ttk.Frame(notebook, padding="20")
        notebook.add(sql_frame, text="SQL Generation")
        
        self._create_sql_tab(sql_frame, entries)
        
        # Logging Tab - NEW
        logging_frame = ttk.Frame(notebook, padding="20")
        notebook.add(logging_frame, text="Logging")
        
        self._create_logging_tab(logging_frame, entries)
        
        # Button frame - inside main frame for guaranteed visibility
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 0))
        
        # Create button container centered in the frame
        button_container = ttk.Frame(button_frame)
        button_container.pack(anchor=tk.CENTER)
        
        # Cancel button (left) - smaller width
        cancel_btn = ttk.Button(
            button_container, 
            text="Cancel", 
            style='LightBlue.TButton',
            width=8,  # Reduced from default
            command=window.destroy
        )
        cancel_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Save button (right) - smaller width
        save_btn = ttk.Button(
            button_container, 
            text="Save Settings", 
            style='LightBlue.TButton',
            width=12,  # Reduced from default
            command=lambda: self._save_changes(entries, window, on_save_callback)
        )
        save_btn.pack(side=tk.LEFT)
        
        # Set initial focus
        notebook.focus_set()

    def _create_database_tab(self, parent, entries):
        """Create database configuration section"""
        # Title
        title_label = ttk.Label(parent, text="Database Configuration", 
                               font=('TkDefaultFont', 10, 'bold'))
        title_label.pack(anchor=tk.W, pady=(0, 15))
        
        # Default Values (combined database and schema)
        defaults_frame = ttk.LabelFrame(parent, text="Default Values", padding="15")
        defaults_frame.pack(fill=tk.X, pady=(0, 15))
        
        # Database Name - horizontal layout
        db_input_frame = ttk.Frame(defaults_frame)
        db_input_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(db_input_frame, text="Database Name:").pack(side=tk.LEFT)
        db_entry = ttk.Entry(db_input_frame, width=25)
        db_entry.insert(0, str(self.config.get("default_database", "")))
        db_entry.pack(side=tk.LEFT, padx=(10, 0))
        entries["default_database"] = db_entry
        
        # Schema Name - horizontal layout
        schema_input_frame = ttk.Frame(defaults_frame)
        schema_input_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(schema_input_frame, text="Schema Name:").pack(side=tk.LEFT)
        schema_entry = ttk.Entry(schema_input_frame, width=25)
        schema_entry.insert(0, str(self.config.get("default_schema", "dbo")))
        schema_entry.pack(side=tk.LEFT, padx=(10, 0))
        entries["default_schema"] = schema_entry
        
        # Use Filename as Table Name checkbox
        use_filename_var = tk.BooleanVar(value=self.config.get("use_filename_as_table_name", True))
        use_filename_cb = ttk.Checkbutton(defaults_frame, text="Use Filename as Table Name", 
                                         variable=use_filename_var)
        use_filename_cb.pack(anchor=tk.W, pady=(0, 10))
        entries["use_filename_as_table_name"] = use_filename_var
        
        # Custom Table Name - horizontal layout
        table_input_frame = ttk.Frame(defaults_frame)
        table_input_frame.pack(fill=tk.X)
        table_label = ttk.Label(table_input_frame, text="Table Name:")
        table_label.pack(side=tk.LEFT)
        table_entry = ttk.Entry(table_input_frame, width=25)
        table_entry.insert(0, str(self.config.get("custom_table_name", "")))
        table_entry.pack(side=tk.LEFT, padx=(10, 0))
        entries["custom_table_name"] = table_entry
        
        # Add callback to enable/disable table name input based on checkbox
        def toggle_table_name(*args):
            if use_filename_var.get():
                table_label.configure(state='disabled')
                table_entry.configure(state='disabled')
            else:
                table_label.configure(state='normal')
                table_entry.configure(state='normal')
        
        # Set initial state based on checkbox value
        toggle_table_name()
        
        # Monitor changes to use_filename_var and call toggle_table_name when it changes
        use_filename_var.trace('w', toggle_table_name)
        
        # Column Settings (moved from processing tab)
        col_frame = ttk.LabelFrame(parent, text="Column Configuration", padding="15")
        col_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Type Inference (moved to top)
        infer_var = tk.BooleanVar(value=self.config.get("default_infer_types", True))
        infer_cb = ttk.Checkbutton(col_frame, text="Enable Data Type Inference", 
                                  variable=infer_var)
        infer_cb.pack(anchor=tk.W, pady=(0, 15))
        entries["default_infer_types"] = infer_var
        
        # Default Column Format - horizontal layout
        format_input_frame = ttk.Frame(col_frame)
        format_input_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(format_input_frame, text="Default Column Format:").pack(side=tk.LEFT)
        
        # Column format options
        column_format_options = ["Source File", "CamelCase", "snake_case", "lowercase", "UPPERCASE"]
        format_var = tk.StringVar(value=self.config.get("default_column_format", "Source File"))
        format_combo = ttk.Combobox(format_input_frame, textvariable=format_var, 
                                   values=column_format_options, width=15, state="readonly")
        format_combo.pack(side=tk.LEFT, padx=(10, 0))
        entries["default_column_format"] = format_var
        
        # Maximum Additional Columns - horizontal layout
        col_input_frame = ttk.Frame(col_frame)
        col_input_frame.pack(fill=tk.X)
        ttk.Label(col_input_frame, text="Maximum Additional Columns:").pack(side=tk.LEFT)
        col_entry = ttk.Entry(col_input_frame, width=5)
        col_entry.insert(0, str(self.config.get("max_additional_columns", 1)))
        col_entry.pack(side=tk.LEFT, padx=(10, 0))
        entries["max_additional_columns"] = col_entry

    def _create_processing_tab(self, parent, entries):
        """Create data processing section"""
        # Title
        title_label = ttk.Label(parent, text="Data Processing Configuration", 
                               font=('TkDefaultFont', 10, 'bold'))
        title_label.pack(anchor=tk.W, pady=(0, 15))
        
        # Sample Settings
        sample_frame = ttk.LabelFrame(parent, text="Data Sampling", padding="15")
        sample_frame.pack(fill=tk.X)
        
        # Auto Preview Data checkbox - NEW ADDITION at the top
        auto_preview_var = tk.BooleanVar(value=self.config.get("auto_preview_data", True))
        auto_preview_cb = ttk.Checkbutton(sample_frame, text="Automatically Preview Data", 
                                         variable=auto_preview_var)
        auto_preview_cb.pack(anchor=tk.W, pady=(0, 15))
        entries["auto_preview_data"] = auto_preview_var
        
        # Default Preview Percentage - horizontal layout
        preview_input_frame = ttk.Frame(sample_frame)
        preview_input_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(preview_input_frame, text="Default Preview Percentage:").pack(side=tk.LEFT)
        preview_entry = ttk.Entry(preview_input_frame, width=5)
        preview_entry.insert(0, str(self.config.get("default_preview_percentage", 10)))
        preview_entry.pack(side=tk.LEFT, padx=(10, 0))
        entries["default_preview_percentage"] = preview_entry
        
        # Sample Percentage for Analysis - horizontal layout
        sample_input_frame = ttk.Frame(sample_frame)
        sample_input_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(sample_input_frame, text="Sample Percentage for Analysis:").pack(side=tk.LEFT)
        sample_entry = ttk.Entry(sample_input_frame, width=5)
        sample_entry.insert(0, str(self.config.get("sample_percentage", 15)))
        sample_entry.pack(side=tk.LEFT, padx=(10, 0))
        entries["sample_percentage"] = sample_entry
        
        # Large File Indicator Threshold - horizontal layout
        large_file_input_frame = ttk.Frame(sample_frame)
        large_file_input_frame.pack(fill=tk.X)
        ttk.Label(large_file_input_frame, text="Large File Indicator (MB):").pack(side=tk.LEFT)
        large_file_entry = ttk.Entry(large_file_input_frame, width=8)
        large_file_entry.insert(0, str(self.config.get("large_file_threshold_mb", 1000)))
        large_file_entry.pack(side=tk.LEFT, padx=(10, 0))
        entries["large_file_threshold_mb"] = large_file_entry

    def _create_sql_tab(self, parent, entries):
        """Create SQL generation section"""
        # Title
        title_label = ttk.Label(parent, text="SQL Generation Options", 
                               font=('TkDefaultFont', 10, 'bold'))
        title_label.pack(anchor=tk.W, pady=(0, 15))
        
        # Statement Generation
        gen_frame = ttk.LabelFrame(parent, text="Statement Generation", padding="15")
        gen_frame.pack(fill=tk.X, pady=(0, 15))
        
        create_var = tk.BooleanVar(value=self.config.get("default_include_create", True))
        create_cb = ttk.Checkbutton(gen_frame, text="Enable CREATE TABLE statements", 
                                   variable=create_var)
        create_cb.pack(anchor=tk.W, pady=(0, 10))
        entries["default_include_create"] = create_var
        
        insert_var = tk.BooleanVar(value=self.config.get("default_include_insert", True))
        insert_cb = ttk.Checkbutton(gen_frame, text="Enable INSERT statements", 
                                   variable=insert_var)
        insert_cb.pack(anchor=tk.W)
        entries["default_include_insert"] = insert_var
        
        # Insert Options
        insert_frame = ttk.LabelFrame(parent, text="Insert Statement Options", padding="15")
        insert_frame.pack(fill=tk.X)
        
        # Default Truncate option (at the top)
        truncate_var = tk.BooleanVar(value=self.config.get("default_truncate", False))
        truncate_cb = ttk.Checkbutton(insert_frame, text="Enable TRUNCATE", 
                                     variable=truncate_var)
        truncate_cb.pack(anchor=tk.W, pady=(0, 10))
        entries["default_truncate"] = truncate_var
        
        batch_var = tk.BooleanVar(value=self.config.get("default_batch_insert", False))
        batch_cb = ttk.Checkbutton(insert_frame, text="Enable Batch INSERT", 
                                  variable=batch_var)
        batch_cb.pack(anchor=tk.W, pady=(0, 10))
        entries["default_batch_insert"] = batch_var
        
        # Batch Size Input - horizontal layout
        batch_input_frame = ttk.Frame(insert_frame)
        batch_input_frame.pack(fill=tk.X, pady=(0, 10))
        batch_label = ttk.Label(batch_input_frame, text="Insert Batch Size:")
        batch_label.pack(side=tk.LEFT)
        batch_entry = ttk.Entry(batch_input_frame, width=12)
        batch_entry.insert(0, str(self.config.get("insert_batch_size", 5000)))
        batch_entry.pack(side=tk.LEFT, padx=(10, 0))
        entries["insert_batch_size"] = batch_entry
        
        # Add callback to enable/disable batch size based on batch checkbox
        def toggle_batch_size(*args):
            if batch_var.get():
                batch_label.configure(state='normal')
                batch_entry.configure(state='normal')
            else:
                batch_label.configure(state='disabled')
                batch_entry.configure(state='disabled')
        
        # Set initial state based on checkbox value
        toggle_batch_size()
        
        # Monitor changes to batch_var and call toggle_batch_size when it changes
        batch_var.trace('w', toggle_batch_size)

    def _create_logging_tab(self, parent, entries):
        """Create logging configuration section"""
        # Title
        title_label = ttk.Label(parent, text="Logging Configuration", 
                               font=('TkDefaultFont', 10, 'bold'))
        title_label.pack(anchor=tk.W, pady=(0, 15))
        
        # Logging Settings
        logging_frame = ttk.LabelFrame(parent, text="Logging Options", padding="15")
        logging_frame.pack(fill=tk.X, pady=(0, 15))
        
        # Enable Logging checkbox - at the top
        enable_logging_var = tk.BooleanVar(value=self.config.get("enable_logging", True))
        enable_logging_cb = ttk.Checkbutton(logging_frame, text="Enable Logging", 
                                           variable=enable_logging_var)
        enable_logging_cb.pack(anchor=tk.W, pady=(0, 15))
        entries["enable_logging"] = enable_logging_var
        
        # Log Directory Settings
        log_dir_frame = ttk.LabelFrame(logging_frame, text="Log Directory", padding="10")
        log_dir_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Custom log directory input
        log_dir_input_frame = ttk.Frame(log_dir_frame)
        log_dir_input_frame.pack(fill=tk.X, pady=(0, 10))
        
        log_dir_label = ttk.Label(log_dir_input_frame, text="Custom Log Directory:")
        log_dir_label.pack(anchor=tk.W, pady=(0, 5))
        
        # Directory path entry and browse button
        dir_entry_frame = ttk.Frame(log_dir_input_frame)
        dir_entry_frame.pack(fill=tk.X)
        
        log_dir_entry = ttk.Entry(dir_entry_frame, width=55)  # Increased width since Browse button is now below
        log_dir_entry.insert(0, str(self.config.get("log_directory", "")))
        log_dir_entry.pack(fill=tk.X, pady=(0, 5))  # Fill width and add bottom padding
        entries["log_directory"] = log_dir_entry
        
        # Browse button below the entry field
        def browse_log_directory():
            directory = filedialog.askdirectory(title="Select Log Directory")
            if directory:
                log_dir_entry.delete(0, tk.END)
                log_dir_entry.insert(0, directory)
        
        log_browse_btn = ttk.Button(dir_entry_frame, text="Browse...", 
                                   style='LightBlue.TButton', width=12,
                                   command=browse_log_directory)
        log_browse_btn.pack(anchor=tk.W)  # Align to left
        
        # Information text
        info_text = ttk.Label(log_dir_frame, text="If left empty, log will be saved to the same directory\nas the SQL script(s)", 
                             font=('TkDefaultFont', 8), foreground='gray')
        info_text.pack(anchor=tk.W, pady=(5, 0))
        
        # Add callback to enable/disable log directory controls based on enable logging checkbox
        def toggle_logging_controls(*args):
            if enable_logging_var.get():
                log_dir_label.configure(state='normal')
                log_dir_entry.configure(state='normal')
                log_browse_btn.configure(state='normal')
                info_text.configure(state='normal')
            else:
                log_dir_label.configure(state='disabled')
                log_dir_entry.configure(state='disabled')
                log_browse_btn.configure(state='disabled')
                info_text.configure(state='disabled')
        
        # Set initial state based on checkbox value
        toggle_logging_controls()
        
        # Monitor changes to enable_logging_var and call toggle_logging_controls when it changes
        enable_logging_var.trace('w', toggle_logging_controls)

    def _save_changes(self, entries, window, on_save_callback):
        """Save configuration changes"""
        try:
            for key, widget in entries.items():
                if isinstance(widget, (tk.BooleanVar, tk.StringVar)):
                    self.config[key] = widget.get()
                elif isinstance(widget, ttk.Entry):
                    val = widget.get()
                    if key in ["default_preview_percentage", "sample_percentage", 
                              "max_additional_columns", "insert_batch_size", "large_file_threshold_mb"]:
                        try:
                            self.config[key] = int(val)
                            if self.config[key] < 1:
                                raise ValueError("Value must be positive")
                        except ValueError:
                            messagebox.showerror(
                                "Invalid Input", 
                                f"{key.replace('_', ' ').title()} must be a positive integer."
                            )
                            return
                    else:
                        self.config[key] = val
            
            # Ensure insert_batch_size exists
            if "insert_batch_size" not in self.config:
                self.config["insert_batch_size"] = 5000
                
            # Ensure logging settings exist
            if "enable_logging" not in self.config:
                self.config["enable_logging"] = True
            if "log_directory" not in self.config:
                self.config["log_directory"] = ""
                
            self.save()
            
            # Show success message
            messagebox.showinfo("Settings Saved", "Your settings have been saved successfully!")
            
            if on_save_callback:
                on_save_callback()
            window.destroy()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")