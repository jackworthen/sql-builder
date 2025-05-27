# config_manager.py
import json
import sys
import os
import tkinter as tk
from tkinter import messagebox, ttk

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
            "insert_batch_size": 5000
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

    def open_settings_window(self, master, on_save_callback=None):
        window = tk.Toplevel(master)
        window.iconbitmap(resource_path('sqlbuilder_icon.ico'))
        window.title("SQL Table Builder Pro - Settings")
        window.geometry("500x500")
        window.resizable(False, False)
        
        # Center the window
        window.transient(master)
        window.grab_set()
        
        # Configure window style
        window.configure(bg='#f0f0f0')
        
        # Create main frame with padding
        main_frame = ttk.Frame(window, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create notebook for tabs
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Dictionary to store all entry widgets
        entries = {}
        
        # Database Configuration Tab
        db_frame = ttk.Frame(notebook, padding="20")
        notebook.add(db_frame, text="Database")
        
        self._create_database_tab(db_frame, entries)
        
        # Data Processing Tab  
        processing_frame = ttk.Frame(notebook, padding="20")
        notebook.add(processing_frame, text="Data Processing")
        
        self._create_processing_tab(processing_frame, entries)
        
        # SQL Generation Tab
        sql_frame = ttk.Frame(notebook, padding="20")
        notebook.add(sql_frame, text="SQL Generation")
        
        self._create_sql_tab(sql_frame, entries)
        
        # Performance Tab
        perf_frame = ttk.Frame(notebook, padding="20")
        notebook.add(perf_frame, text="Performance")
        
        self._create_performance_tab(perf_frame, entries)
        
        # Button frame - inside main frame for guaranteed visibility
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 0))
        
        # Style the buttons
        style = ttk.Style()
        style.configure('Action.TButton', padding=(15, 8))
        
        # Create button container centered in the frame
        button_container = ttk.Frame(button_frame)
        button_container.pack(anchor=tk.CENTER)
        
        # Cancel button (left)
        cancel_btn = ttk.Button(
            button_container, 
            text="Cancel", 
            style='Action.TButton',
            command=window.destroy
        )
        cancel_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Save button (right)
        save_btn = ttk.Button(
            button_container, 
            text="Save Settings", 
            style='Action.TButton',
            command=lambda: self._save_changes(entries, window, on_save_callback)
        )
        save_btn.pack(side=tk.LEFT)
        
        # Set initial focus
        notebook.focus_set()

    def _create_database_tab(self, parent, entries):
        """Create database configuration section"""
        # Title
        title_label = ttk.Label(parent, text="Database Connection Settings", 
                               font=('TkDefaultFont', 10, 'bold'))
        title_label.pack(anchor=tk.W, pady=(0, 15))
        
        # Default Database
        db_frame = ttk.LabelFrame(parent, text="Default Database", padding="15")
        db_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(db_frame, text="Database Name:").pack(anchor=tk.W)
        db_entry = ttk.Entry(db_frame, width=40)
        db_entry.insert(0, str(self.config.get("default_database", "")))
        db_entry.pack(fill=tk.X, pady=(5, 0))
        entries["default_database"] = db_entry
        
        # Default Schema
        schema_frame = ttk.LabelFrame(parent, text="Default Schema", padding="15")
        schema_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(schema_frame, text="Schema Name:").pack(anchor=tk.W)
        schema_entry = ttk.Entry(schema_frame, width=40)
        schema_entry.insert(0, str(self.config.get("default_schema", "dbo")))
        schema_entry.pack(fill=tk.X, pady=(5, 0))
        entries["default_schema"] = schema_entry

    def _create_processing_tab(self, parent, entries):
        """Create data processing section"""
        # Title
        title_label = ttk.Label(parent, text="Data Processing Configuration", 
                               font=('TkDefaultFont', 10, 'bold'))
        title_label.pack(anchor=tk.W, pady=(0, 15))
        
        # Column Settings
        col_frame = ttk.LabelFrame(parent, text="Column Configuration", padding="15")
        col_frame.pack(fill=tk.X, pady=(0, 15))
        
        ttk.Label(col_frame, text="Maximum Additional Columns:").pack(anchor=tk.W)
        col_entry = ttk.Entry(col_frame, width=20)
        col_entry.insert(0, str(self.config.get("max_additional_columns", 1)))
        col_entry.pack(anchor=tk.W, pady=(5, 10))
        entries["max_additional_columns"] = col_entry
        
        # Type Inference
        infer_var = tk.BooleanVar(value=self.config.get("default_infer_types", True))
        infer_cb = ttk.Checkbutton(col_frame, text="Automatically infer column data types", 
                                  variable=infer_var)
        infer_cb.pack(anchor=tk.W)
        entries["default_infer_types"] = infer_var
        
        # Sample Settings
        sample_frame = ttk.LabelFrame(parent, text="Data Sampling", padding="15")
        sample_frame.pack(fill=tk.X)
        
        ttk.Label(sample_frame, text="Default Preview Percentage (%):").pack(anchor=tk.W)
        preview_entry = ttk.Entry(sample_frame, width=20)
        preview_entry.insert(0, str(self.config.get("default_preview_percentage", 10)))
        preview_entry.pack(anchor=tk.W, pady=(5, 10))
        entries["default_preview_percentage"] = preview_entry
        
        ttk.Label(sample_frame, text="Sample Percentage for Analysis (%):").pack(anchor=tk.W)
        sample_entry = ttk.Entry(sample_frame, width=20)
        sample_entry.insert(0, str(self.config.get("sample_percentage", 15)))
        sample_entry.pack(anchor=tk.W, pady=(5, 0))
        entries["sample_percentage"] = sample_entry

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
        create_cb = ttk.Checkbutton(gen_frame, text="Include CREATE TABLE statements", 
                                   variable=create_var)
        create_cb.pack(anchor=tk.W, pady=(0, 10))
        entries["default_include_create"] = create_var
        
        insert_var = tk.BooleanVar(value=self.config.get("default_include_insert", True))
        insert_cb = ttk.Checkbutton(gen_frame, text="Include INSERT statements", 
                                   variable=insert_var)
        insert_cb.pack(anchor=tk.W)
        entries["default_include_insert"] = insert_var
        
        # Insert Options
        insert_frame = ttk.LabelFrame(parent, text="Insert Statement Options", padding="15")
        insert_frame.pack(fill=tk.X)
        
        batch_var = tk.BooleanVar(value=self.config.get("default_batch_insert", False))
        batch_cb = ttk.Checkbutton(insert_frame, text="Use batch insert statements", 
                                  variable=batch_var)
        batch_cb.pack(anchor=tk.W, pady=(0, 10))
        entries["default_batch_insert"] = batch_var

    def _create_performance_tab(self, parent, entries):
        """Create performance section"""
        # Title
        title_label = ttk.Label(parent, text="Performance Settings", 
                               font=('TkDefaultFont', 10, 'bold'))
        title_label.pack(anchor=tk.W, pady=(0, 15))
        
        # Batch Processing
        batch_frame = ttk.LabelFrame(parent, text="Batch Processing", padding="15")
        batch_frame.pack(fill=tk.X)
        
        ttk.Label(batch_frame, text="Insert Batch Size (rows per batch):").pack(anchor=tk.W)
        
        # Add description
        desc_label = ttk.Label(batch_frame, 
                              text="Higher values improve performance but use more memory",
                              font=('TkDefaultFont', 8),
                              foreground='gray')
        desc_label.pack(anchor=tk.W, pady=(0, 10))
        
        batch_entry = ttk.Entry(batch_frame, width=20)
        batch_entry.insert(0, str(self.config.get("insert_batch_size", 5000)))
        batch_entry.pack(anchor=tk.W)
        entries["insert_batch_size"] = batch_entry

    def _save_changes(self, entries, window, on_save_callback):
        """Save configuration changes"""
        try:
            for key, widget in entries.items():
                if isinstance(widget, tk.BooleanVar):
                    self.config[key] = widget.get()
                elif isinstance(widget, ttk.Entry):
                    val = widget.get()
                    if key in ["default_preview_percentage", "sample_percentage", 
                              "max_additional_columns", "insert_batch_size"]:
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
                
            self.save()
            
            # Show success message
            messagebox.showinfo("Settings Saved", "Your settings have been saved successfully!")
            
            if on_save_callback:
                on_save_callback()
            window.destroy()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")