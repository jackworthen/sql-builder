# 📘 SQL Table Builder Pro

[![Version](https://img.shields.io/badge/version-1.3.1-blue.svg)](https://github.com/your-repo)
[![Build Date](https://img.shields.io/badge/build-2025--04--24-lightgrey)](https://github.com/your-repo)
[![Platform](https://img.shields.io/badge/platform-Windows-blue)](https://github.com/your-repo)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## What is SQL Table Builder Pro?

**SQL Table Builder Pro** is a simple and intuitive desktop tool that helps users turn spreadsheet-style data files into ready-to-use SQL scripts. With just a few clicks, you can preview your data, customize table settings, and automatically generate scripts to create and fill database tables. Whether you're setting up a new database or just need quick scripts from a CSV file, this app makes the process easy—no coding required.

> **Note:** The current build only supports single character delimiters.

---

## 🗁 File Selection

- Click **File → Open** or **Browse...** to select a `.csv`, `.txt`, or `.dat` file.
- The application will try to **infer the delimiter** and suggest a table name based on the file name.
- Use the **Preview %** input to display a sample of the data.

---

## 🔧 Settings

- Accessible via **Edit → Settings**.
- Customize:
  - Default schema and database name
  - Whether to include `CREATE` or `INSERT` scripts
  - Default preview and data sampling %
  - Batch insert size
  - Maximum number of additional columns that can be added

> 💡 *Settings are saved to `config.json` and persist across sessions.*

---

## ✍️ Table & Column Definition

- Manually adjust column names and SQL data types.
- Click **Add Column** to add extra fields (up to the configured limit).
- Choose from naming conventions like `snake_case`, `CamelCase`, `lowercase`, and `UPPERCASE`.
- Use **INT IDENTITY** or **UNIQUEIDENTIFIER** for primary keys.

> 💡 *In order to set **INT IDENTITY** or **UNIQUEIDENTIFIER** as the primary key data type, you must first add a new column.*

---

## 🧠 Data Type Inference

- If enabled, the app will analyze sample data to suggest types like `INT`, `FLOAT`, `DATETIME`, etc.
- You can adjust the **Sample Percentage** under **Edit → Settings** to have the application sample a smaller or larger portion of the file when inferring the data type.
- Click **Reset Data Types** if you’ve made manual changes and want to restore inferred values.

---

## 📜 Script Generation

- Choose which SQL statements to generate: `CREATE TABLE`, `INSERT INTO`, or both.
- Use `TRUNCATE` before inserting if desired. The `TRUNCATE` option is only enabled when `INSERT INTO` is checked.
- **Batch INSERT** allows grouping rows into smaller chunks (e.g. 5000 rows at a time). The **Batch INSERT** option is only available when `INSERT INTO` is checked.
- Save output files anywhere via a file dialog.

---

## 🛈 About

**SQL Table Builder Pro**  
Version 1.3.1  
Build Date: 2025-04-24  
Developed by Jack Worthen
