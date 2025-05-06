# SQL Table Builder Pro
# Created By: Jack Worthen
# Description: Reads a data file and creates SQL scripts for creating a table and inserting data into table.

from config_manager import ConfigManager

import sys
import os

def resource_path(filename):
    """ Get absolute path to resource, works for dev and for PyInstaller bundle """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.abspath("."), filename)

import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
import csv
import subprocess
import sys
import os
from collections import Counter

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
        self.master.geometry("750x470")  
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
  
    def build_file_selection_screen(self):
        self.master.iconbitmap(resource_path('sqlbuilder_icon.ico'))  # This sets the icon
        self.use_identity.set(False)
        self.use_guid.set(False)
        self.delimiter.set("")  # Clear the delimiter field
        self.file_path.set("")  # Clear previously selected file
        self.master.geometry("750x470")
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
        file_menu.add_command(label="🗁 Open", command=self.browse_file)
        file_menu.add_separator() 
        file_menu.add_command(label="⏻ Exit", command=self.master.quit)
        menubar.add_cascade(label="File", menu=file_menu)
        
        # Edit menu
        edit_menu = tk.Menu(menubar, tearoff=0, **safe_menu_opts)
        edit_menu.add_command(label="🔧 Settings", command=lambda: self.config_mgr.open_settings_window(self.master, self.apply_config_settings))
        menubar.add_cascade(label="Edit", menu=edit_menu)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0, **safe_menu_opts)
        help_menu.add_command(label="📘 Documentation", command=self.show_help)
        help_menu.add_separator() 
        help_menu.add_command(label="🛈 About", command=self.show_about)
        menubar.add_cascade(label="Help", menu=help_menu)

        main_frame = tk.Frame(self.master, padx=20, pady=20)
        main_frame.pack(expand=True, fill="both")
       
        # Heading
        tk.Label(main_frame, text="SQL Table Builder Pro", font=("Arial", 16, "bold")).pack(anchor="w", pady=(0, 10))

        # File selection group
        file_group = tk.LabelFrame(main_frame, text="Select Source File", padx=10, pady=10)
        file_group.pack(fill="x", pady=5)

        file_frame = tk.Frame(file_group)
        file_frame.pack(fill="x", pady=5)
        file_entry = tk.Entry(file_frame, textvariable=self.file_path, width=50)
        file_entry.pack(side="left", fill="x", expand=True, padx=(0, 5))
        tk.Button(file_frame, text="Browse...", underline=0, command=self.browse_file).pack(side="left")

        # Delimiter
        delimiter_frame = tk.Frame(file_group)
        delimiter_frame.pack(fill="x", pady=(5, 0))
        tk.Label(delimiter_frame, text="Delimiter:").pack(side="left")
        delimiter_entry = tk.Entry(delimiter_frame, textvariable=self.delimiter, width=5, justify="center")
        delimiter_entry.pack(side="left", padx=5)
        self.infer_checkbox = tk.Checkbutton(
            delimiter_frame, text="Infer Data Types ", variable=self.infer_types_var
        )
        self.infer_checkbox.pack(side="left", padx=10)
        tk.Label(delimiter_frame, text="Preview %:").pack(side="left", padx=(10, 2))
        tk.Entry(delimiter_frame, textvariable=self.preview_percentage_var, width=5, justify="center").pack(side="left")
        self.show_button = tk.Button(delimiter_frame, text="Show", underline=0, width=5, state="disabled", command=self.on_apply_preview_percentage)
        self.show_button.pack(side="left", padx=(5, 0))
                
        # Action button
        self.preview_frame = tk.LabelFrame(main_frame, text="Data Preview", padx=5, pady=5)
        self.preview_frame.pack(fill="both", expand=True, pady=(10, 10))

        action_frame = tk.Frame(main_frame)
        action_frame.pack(pady=15)
        self.next_button = tk.Button(action_frame, text="Next >", underline=0, width=15, state="disabled", command=self.process_file)
        self.next_button.pack()
                
    def browse_file(self):
        filetypes = [("Data Files", "*.csv *.txt *.dat"), ("All Files", "*.*")]
        selected_path = filedialog.askopenfilename(title="Open File", filetypes=filetypes)
        if selected_path:
            self.file_path.set(selected_path)
            self.infer_delimiter()
            default_name = os.path.splitext(os.path.basename(selected_path))[0]
            self.table_name.set(default_name)
        # Preview will only load when "Set" is clicked
            self.next_button.config(state="normal")
            self.show_button.config(state="normal")

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

    def process_file(self):
        path = self.file_path.get()
        if not path:
            return

        try:
            with open(path, newline='') as f:
                delimiter_val = self.delimiter.get()
                delimiter_val = "\t" if delimiter_val == "\\t" else delimiter_val
                reader = csv.reader(f, delimiter=delimiter_val)
                self.headers = next(reader)
        except Exception as e:
            return

        self.build_column_type_screen()
    
    def build_column_type_screen(self):
        self.additional_column_count = 0
        self.master.geometry("510x800")
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
        #tk.Label(db_frame, text="(optional)").pack(side="left", padx=5)

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

        # 🟢 Moved Add Column and Reset buttons inline (order switched)
        self.add_column_button = tk.Button(options_frame, text="Add Column", underline=0, command=self.add_new_column_row)
        self.add_column_button.pack(side="left", padx=10)
        self.reset_button = tk.Button(options_frame, text="Reset Data Types", underline=0, command=self.set_inferred_types, state="disabled")
        self.reset_button.pack(side="left", padx=5)
        
        # Row for renaming dropdown and set button
        rename_frame = tk.Frame(settings_frame)
        rename_frame.pack(fill="x", pady=3)
        tk.Label(rename_frame, text="Format Columns:").pack(side="left", padx=(5, 2))
        self.naming_style_var = tk.StringVar(value="")
        self.naming_combo = ttk.Combobox(rename_frame, textvariable=self.naming_style_var, values=["CamelCase", "snake_case", "lowercase", "UPPERCASE"], width=15, state="readonly")
        self.naming_combo.pack(side="left", padx=2)
        tk.Button(rename_frame, text="Set", underline=0, width=5, command=self.apply_column_naming_convention).pack(side="left", padx=5)    
        
        # Disable checkboxes initially
        self.identity_checkbox.config(state="disabled")
        self.guid_checkbox.config(state="disabled")
        self.identity_guid_enabled = False

        # === COLUMN DEFINITION SECTION ===
        column_frame = tk.LabelFrame(self.master, text="Define Table Columns", padx=10, pady=10)
        column_frame.pack(fill="both", expand=True, padx=10, pady=5)

        canvas = tk.Canvas(column_frame)
        scrollbar = tk.Scrollbar(column_frame, orient="vertical", command=canvas.yview)
        self.scrollable_frame = tk.Frame(canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

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
            type_combo.bind("<<ComboboxSelected>>", lambda e: self.enable_reset_button())

            null_checkbox = tk.Checkbutton(row, variable=null_var, command=lambda idx=index: self.update_null_states(idx))
            null_checkbox.pack(side="left")

            self.pk_vars.append(pk_var)
            self.null_vars.append(null_var)
            self.pk_checkboxes.append(pk_checkbox)
            self.null_checkboxes.append(null_checkbox)
            self.column_entries.append(name_entry)
            self.type_entries.append(type_combo)

        if self.infer_types_var.get():
            self.set_inferred_types()

        # Column Count (bugged)
        #tk.Label(self.scrollable_frame, text=f"Columns: {len(self.headers)}", anchor="w", fg="blue").pack(pady=8, anchor="w")

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
        tk.Button(checkbox_row, text="Save", underline=2, width=15, command=self.handle_generate_scripts).pack(side="right", padx=10)
        self.update_truncate_enable_state()

# === BACK AND EXIT BUTTONS ===
        back_frame = tk.Frame(self.master)
        back_frame.pack(pady=(5, 15))
        tk.Button(back_frame, text="< Back", underline=2, width=15, command=self.build_file_selection_screen).pack(side="left", padx=10)
        tk.Button(back_frame, text="Exit", underline=0, width=15, command=self.master.quit).pack(side="left", padx=10)
        
    def handle_generate_scripts(self):
        if self.include_create_script.get():
            self.generate_sql_file()
        if self.include_insert_script.get():
            self.generate_insert_statements()
        
    
    
    
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
        if self.reset_button['state'] == "disabled":
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
  
    def generate_insert_statements(self):
        from tkinter import filedialog
        import csv

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
        values_lines = []
        path = self.file_path.get()

        try:
            with open(path, newline='') as f:
                delimiter_val = self.delimiter.get()
                delimiter_val = "	" if delimiter_val == "\t" else delimiter_val
                reader = csv.reader(f, delimiter=delimiter_val)
                next(reader)  # skip header
                for row in reader:
                    extra_count = len(column_types) - len(row)
                    if extra_count > 0:
                        row = [''] * extra_count + row
                    values_lines.append(self.format_insert_values(row, column_types))
        except Exception:
            return

        if not values_lines:
            return

        db_name = self.database_name.get().strip()
        script_lines = []

        if db_name:
            script_lines.append(f"USE [{db_name}];")
            script_lines.append("GO")
            script_lines.append("")

        if self.truncate_before_insert.get():
            script_lines.append(f"TRUNCATE TABLE {full_table};")
            script_lines.append("GO")
            script_lines.append("")

        insert_header = f"INSERT INTO {full_table} ({', '.join(f'[{c}]' for c in col_names)})\nVALUES"
        if self.batch_insert_var.get():
            for i in range(0, len(values_lines), self.insert_batch_size):
                chunk = values_lines[i:i + self.insert_batch_size]
                script_lines.append(insert_header)
                script_lines.append(",\n".join(chunk) + ";\nGO")
        else:
            script_lines.append(insert_header)
            script_lines.append(",\n".join(values_lines) + ";\nGO")

        script = "\n".join(script_lines)

        default_filename = f"insert_into_{table_name}.sql"
        file_path = filedialog.asksaveasfilename(defaultextension=".sql", initialfile=default_filename, filetypes=[("SQL Files", "*.sql")])
        if file_path:
            with open(file_path, 'w') as f:
                f.write(script)

    def set_inferred_types(self):
        import csv
        from datetime import datetime
        from tkinter import messagebox

        def infer_sql_type(column_values):
            is_int = True
            is_float = True
            is_bit = True
            is_date = True
            max_len = 0

            for val in column_values:
                val = val.strip()
                max_len = max(max_len, len(val))

                try:
                    int(val)
                except ValueError:
                    is_int = False

                try:
                    float(val)
                except ValueError:
                    is_float = False

                if val.lower() not in ['0', '1']:
                    is_bit = False

                try:
                    datetime.fromisoformat(val)
                except ValueError:
                    is_date = False

            if is_bit:
                return "BIT"
            elif is_int:
                return "INT"
            elif is_float:
                return "FLOAT"
            elif is_date:
                return "DATETIME"
            elif max_len <= 10:
                return "NVARCHAR(10)"
            elif 10 < max_len <=50:
                return "NVARCHAR(50)"
            elif 50 < max_len <=100:
                return "NVARCHAR(100)"             
            elif 100 < max_len <= 255:
                return "NVARCHAR(255)"
            elif max_len <= 4000:
                return f"NVARCHAR({max_len})"
            else:
                return "NVARCHAR(MAX)"

        path = self.file_path.get()
        try:
            with open(path, newline='') as f:
                delimiter_val = self.delimiter.get()
                delimiter_val = "\t" if delimiter_val == "\\t" else delimiter_val
                reader = csv.reader(f, delimiter=delimiter_val)
                headers = next(reader)
                data_rows = list(reader)
            total_rows = len(data_rows)
            sample_percentage = self.sample_percentage / 100.0  # Use % of rows for inference
            sample_count = max(1, int(total_rows * sample_percentage))
            sample_rows = data_rows[:sample_count]

            sample_columns = list(map(list, zip(*sample_rows)))

            #sample_columns = list(map(list, zip(*data_rows)))
            inferred_types = [infer_sql_type(col) for col in sample_columns]

            for i, inferred_type in enumerate(inferred_types):
                combo = self.type_entries[-len(inferred_types) + i]
                combo.delete(0, "end")
                combo.insert(0, inferred_type)

            self.reset_button.config(state="disabled")

        except Exception as e:
            self.reset_button.config(state="disabled")
            messagebox.showerror("Error", f"Failed to infer types: {e}")

    def toggle_infer_types(self):
        if self.infer_types_var.get():
            if self.infer_types_var.get():
                self.set_inferred_types()
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

    def on_apply_preview_percentage(self):
        try:
            percent = int(self.preview_percentage_var.get())
            if 1 <= percent <= 100:
                self.update_preview_table(percentage=percent)
            else:
                raise ValueError
        except ValueError:
            from tkinter import messagebox
            messagebox.showerror("Invalid Input", "Please enter an integer between 1 and 100.")

    def update_preview_table(self, percentage=1):  #Default percentage for record preview
        import csv
        for widget in self.preview_frame.winfo_children():
            widget.destroy()

        path = self.file_path.get()
        if not path or not os.path.isfile(path):
            return

        try:
            with open(path, newline='') as f:
                delimiter_val = self.delimiter.get()
                delimiter_val = "	" if delimiter_val == "\t" else delimiter_val
                reader = csv.reader(f, delimiter=delimiter_val)
                headers = next(reader)
                all_rows = list(reader)
                total = len(all_rows)
                count = max(1, int((percentage / 100) * total))
                rows = all_rows[:count]

            container = tk.Frame(self.preview_frame)
            container.pack(fill="both", expand=True)

            x_scroll = tk.Scrollbar(container, orient="horizontal")
            y_scroll = tk.Scrollbar(container, orient="vertical")

            tree = ttk.Treeview(
                container,
                columns=headers,
                show='headings',
                height=5,
                xscrollcommand=x_scroll.set,
                yscrollcommand=y_scroll.set
            )

            x_scroll.config(command=tree.xview)
            y_scroll.config(command=tree.yview)

            x_scroll.pack(side="bottom", fill="x")
            y_scroll.pack(side="right", fill="y")
            tree.pack(side="left", fill="both", expand=True)

            for header in headers:
                tree.heading(header, text=header, anchor='w')
                tree.column(header, width=120, anchor='w')

            for row in rows:
                tree.insert("", "end", values=row)

        except Exception as e:
            pass

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

    
    def show_help(self):
        import webbrowser
        import os
        import sys
        try:
            if getattr(sys, 'frozen', False):
                base_path = sys._MEIPASS
            else:
                base_path = os.path.dirname(os.path.abspath(__file__))

            help_file_path = os.path.join(base_path, "help.html")

            if not os.path.isfile(help_file_path):
                raise FileNotFoundError(f"File not found: {help_file_path}")

            webbrowser.open(f"file://{help_file_path}")
        except Exception as e:
            import tkinter.messagebox
            tkinter.messagebox.showerror("Error", f"Could not open help file: {e}")


    def show_about(self):
        import tkinter.messagebox
        tkinter.messagebox.showinfo(
            "About",
            "SQL Table Builder Pro\n\nVersion: 1.3.1\nBuild Date: 2025-04-20 17:01:00\n\n© 2025 Jack Worthen"
        )

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

        # Try to insert after header row if possible
        children = self.scrollable_frame.winfo_children()
        # Insert the row right after the header row for alignment
        header_row = self.scrollable_frame.winfo_children()[0]
        row.pack(after=header_row)

        pk_var = tk.BooleanVar()
        null_var = tk.BooleanVar(value=True)

        name_entry = tk.Entry(row, width=30)
        pk_checkbox = tk.Checkbutton(row, variable=pk_var, command=lambda: self.update_pk_states(0))
        pk_checkbox.pack(side="left")

        name_entry.pack(side="left")

        type_combo = ttk.Combobox(row, width=27, values=self.sql_data_types)
        type_combo.pack(side="left", padx=5)
        type_combo.bind("<<ComboboxSelected>>", lambda e: self.enable_reset_button())

        null_checkbox = tk.Checkbutton(row, variable=null_var)
        null_checkbox.pack(side="left")

        self.pk_vars.insert(0, pk_var)
        self.null_vars.insert(0, null_var)
        self.pk_checkboxes.insert(0, pk_checkbox)
        self.null_checkboxes.insert(0, null_checkbox)
        self.column_entries.insert(0, name_entry)
        self.type_entries.insert(0, type_combo)

        self.additional_column_count += 1
        if self.additional_column_count >= self.max_additional_columns:
            self.add_column_button.config(state="disabled")

        # Update index bindings again
        for idx, (pk_cb, null_cb) in enumerate(zip(self.pk_checkboxes, self.null_checkboxes)):
            pk_cb.config(command=lambda i=idx: self.update_pk_states(i))
            null_cb.config(command=lambda i=idx: self.update_null_states(i))

if __name__ == "__main__":
    root = tk.Tk()
    app = SQLTableBuilder(root)
    root.mainloop()