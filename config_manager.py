# config_manager.py
import json

import sys
import os

def resource_path(filename):
    """ Get absolute path to resource, works for dev and for PyInstaller bundle """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.abspath("."), filename)

import os
import tkinter as tk
from tkinter import messagebox

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
        window.title(" Edit Settings")
        window.geometry("350x365")

        entries = {}

        def save_changes():
            for key, widget in entries.items():
                if isinstance(widget, tk.BooleanVar):
                    val = widget.get()
                    self.config[key] = bool(val)
                elif isinstance(widget, tk.Entry):
                    val = widget.get()
                    if key in ["default_preview_percentage", "sample_percentage"]:
                        try:
                            self.config[key] = int(val)
                        except ValueError:
                            messagebox.showerror("Invalid Input", f"{key} must be an integer.")
                            return
                    else:
                        self.config[key] = val
            self.save()
            if on_save_callback:
                on_save_callback()
            window.destroy()

        row = 0
        for key, val in self.config.items():
            if "insert_batch_size" not in self.config:
                self.config["insert_batch_size"] = 500
            label = tk.Label(window, text=key.replace("_", " ").title())
            label.grid(row=row, column=0, sticky="w", padx=10, pady=5)

            if isinstance(val, bool):
                var = tk.BooleanVar(value=val)
                cb = tk.Checkbutton(window, variable=var)
                cb.grid(row=row, column=1, sticky="w")
                entries[key] = var
            else:
                ent = tk.Entry(window)
                ent.insert(0, str(val))
                ent.grid(row=row, column=1, padx=10)
                entries[key] = ent
            row += 1

        save_btn = tk.Button(window, text="Save", command=save_changes)
        save_btn.grid(row=row, column=0, columnspan=2, pady=15)
        