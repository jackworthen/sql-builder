# 🚀 SQL Table Builder Pro

**🎯 Transform your data files into SQL magic with just a few clicks!**

---

## 🌟 What is SQL Table Builder Pro?

SQL Table Builder Pro is a powerful, user-friendly desktop application that transforms your messy data files into pristine SQL scripts. Whether you're dealing with tiny datasets or massive files, this tool has got your back!

## 🎮 Features That'll Make You Smile

### 🧠 **Smart Data Type Inference**
- Automatically detects data types from your files with statistical sampling
- Supports INT, FLOAT, DATETIME, VARCHAR, BIT, and more
- Intelligent type reset for data-driven columns only
- Customizable type inference with configurable sample percentages
- Dynamic VARCHAR length optimization based on actual data content

### ⚡ **Lightning-Fast Performance**
- **Chunked Processing**: Handle files with millions of rows without breaking a sweat
- **Intelligent Caching**: Loads data once, uses it everywhere
- **Progressive Loading**: See your data while it's still loading
- **Background Threading**: UI stays responsive during heavy operations
- **Memory-Efficient Large File Handling**: Configurable file size thresholds (MB-based)

### 🎨 **Beautiful & Intuitive Interface**
- Clean, modern GUI with light blue theme that's easy on the eyes
- Enhanced data preview with alternating row colors and smart column sizing
- Real-time progress dialogs for long operations
- Responsive design that adapts to your data
- **Menu System**: File, Edit, and Help menus with full keyboard shortcut support
- **Large File Indicators**: Visual warnings and file size display for large datasets
- **Tabbed Settings Interface**: Organized configuration across multiple categories

### 🛠️ **Powerful Customization**
- **Dynamic Primary Key Support**: INT IDENTITY and UNIQUEIDENTIFIER options appear automatically in dropdowns when primary key is selected
- **Column Naming Conventions**: CamelCase, snake_case, UPPERCASE, lowercase transformations
- **Flexible Column Management**: Add custom columns with independent type selection
- **Smart Type Reset**: Reset only original data-driven column types, preserve manual additions
- **Batch INSERT Statements**: Configurable batch sizes for optimal performance
- **Schema & Database Support**: Full SQL Server compatibility
- **Persistent Settings**: Platform-specific configuration storage with JSON persistence

### 📁 **Advanced File Handling**
- **Multi-format Support**: CSV, TXT, DAT, JSON, and custom delimiter files (|, ;, :, ^, tab, space, and more!)
- **Auto-delimiter Detection**: Intelligent detection of file delimiters with descriptive display names
- **JSON Processing**: Full nested object flattening with dot notation, array handling (objects and primitives)
- **Large File Optimization**: Configurable MB-based thresholds with automatic handling
- **Configurable Preview**: Adjustable preview percentages for data inspection with auto-preview option
- **Memory Efficient**: Optimized for minimal memory footprint with chunked processing

### 📊 **Comprehensive Logging & Audit Trail**
- **Detailed Operation Logs**: Complete audit trail of all script generation operations
- **Data Validation**: Automatic row count validation between source and processed data
- **File Information**: Source file details, sizes, and processing statistics
- **Script Documentation**: Generated script locations, sizes, and row counts
- **Performance Tracking**: Processing times and operation success/failure status
- **Flexible Log Storage**: Custom log directories or automatic placement with SQL scripts
- **Enable/Disable Logging**: Full control over logging functionality

---

## 🚀 Quick Start

### 📋 Prerequisites

```bash
Python 3.8+
tkinter (usually comes with Python)
```

### 💻 Installation

1. **Clone this repo:**
   ```bash
   git clone https://github.com/jackworthen/sql-builder.git
   cd sql-builder
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Launch the magic:**
   ```bash
   python sqlbuilder.py
   ```

### 🎯 Usage in 3 Easy Steps

1. **📂 Select Your Data File**
   - Click "Browse" and select your data file
   - Watch as the delimiter is automatically detected
   - Preview your data with adjustable sample percentage (1-100%)
   - Auto-preview option loads data immediately upon file selection

2. **⚙️ Configure Your Table**
   - Set database, schema, and table names
   - Let smart type inference work its magic, or customize manually
   - Select primary key columns to unlock INT IDENTITY and UNIQUEIDENTIFIER options
   - Add custom columns as needed
   - Apply column naming conventions (CamelCase, snake_case, etc.)

3. **💾 Generate SQL Scripts**
   - CREATE TABLE statements with proper constraints
   - INSERT INTO statements with optional batching and TRUNCATE
   - Save scripts and use in your favorite SQL environment!
   - **📋 Automatic logging** creates detailed operation records for validation and audit purposes

---

## 🔧 Advanced Features

### 🎛️ Configuration Options

| Feature | Description |
|---------|-------------|
| **Dynamic Primary Keys** | INT IDENTITY and UNIQUEIDENTIFIER automatically available when PK is selected |
| **Batch INSERT** | Configurable batch sizes (default: 5000 rows) |
| **Large File Mode** | Configurable MB-based threshold with automatic optimization |
| **Smart Type Inference** | Statistical sampling with customizable percentage |
| **TRUNCATE Option** | Optional table truncation before INSERT with visual warning |
| **Multi-threading** | Background processing for smooth UI experience |
| **Intelligent Reset** | Reset only original column types, preserve manual additions |
| **Auto-Preview Data** | Configurable automatic data preview on file selection |
| **Table Naming** | Use filename or custom table names |
| **Platform Config** | Cross-platform settings storage in appropriate system directories |
| **Comprehensive Logging** | Detailed operation logs with data validation and audit trail |
| **Custom Log Directory** | Flexible log file placement with automatic fallback |

### 🎨 Column Management

#### Naming Conventions
Transform your column names instantly:

```
Original: "User Name"
CamelCase: UserName
snake_case: user_name
UPPERCASE: USERNAME
lowercase: username
Source File: User Name (original)
```

#### Primary Key Types
When you select a column as a primary key (🔑), these options automatically appear:
- **INT IDENTITY**: Auto-incrementing integer primary key
- **UNIQUEIDENTIFIER**: GUID-based primary key

#### Smart Type Reset
- **Original Columns**: Reset to automatically inferred types from your data
- **Manual Columns**: Preserve your custom types (never reset)
- **Button State**: Only enabled when original column types are modified

### ⚡ Performance Features

- **Smart Caching**: Files loaded once and cached for multiple operations
- **Chunked Reading**: Large files processed in manageable chunks
- **Asynchronous Processing**: UI remains responsive during long operations
- **Progress Tracking**: Real-time feedback for file operations
- **Memory Optimization**: Efficient handling of large datasets
- **Generator-based Processing**: Stream processing for extremely large files

### 📊 Logging & Audit Features

#### What Gets Logged
Every script generation operation creates a comprehensive log containing:

- **📅 Operation Timestamp**: Exact date and time of script generation
- **📁 Source File Details**: File path, name, type, size, and total row count
- **🗂️ Table Configuration**: Database, schema, table name, and column details
- **🔧 Processing Settings**: Type inference, column format, batch settings
- **📄 Generated Scripts**: File names, paths, sizes, and row counts
- **✅ Data Validation**: Source vs processed row count verification
- **⏱️ Performance Metrics**: Total processing time
- **🚨 Error Tracking**: Any issues or warnings during processing

#### Log Configuration
- **Enable/Disable**: Full control over logging functionality
- **Custom Directory**: Specify where log files should be saved
- **Automatic Placement**: Logs saved alongside SQL scripts if no custom directory specified
- **Timestamped Files**: Each operation gets a unique log file with timestamp

#### Sample Log Output
```
================================================================================
SQL TABLE BUILDER PRO - OPERATION LOG
================================================================================

Operation Date/Time: 2024-01-15 14:30:45
Log File: SQLTableBuilder_employees_20240115_143045.log

SOURCE FILE INFORMATION:
----------------------------------------
File Path: C:\Data\employees.csv
File Name: employees.csv
File Type: CSV
File Size: 2.5 MB
Total Rows in Source: 10,000
Delimiter: comma ( , )

SCRIPTS GENERATED:
----------------------------------------
✓ CREATE TABLE script generated
  File: create_table_employees.sql
  Path: C:\Scripts\create_table_employees.sql
  Size: 1.2 KB
✓ INSERT statements generated
  File: insert_into_employees.sql
  Path: C:\Scripts\insert_into_employees.sql
  Size: 1.8 MB
  Rows Processed: 10,000

DATA VALIDATION:
----------------------------------------
✓ Row count validation PASSED
  Source rows: 10,000
  Processed rows: 10,000

OPERATION SUMMARY:
----------------------------------------
Scripts Generated: CREATE TABLE, INSERT
Total Processing Time: 2.3 seconds
Operation Status: SUCCESS
```

### 🎹 Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| **Ctrl+O** | Open File |
| **Ctrl+S** | Settings |
| **Ctrl+E** | Exit |
| **Ctrl+D** | Documentation (GitHub) |

---

## 📝 Example Usage

### Input CSV:
```csv
user_id,user_name,email,created_date,is_active
1,John Doe,john@email.com,2023-01-15,true
2,Jane Smith,jane@email.com,2023-01-16,false
```

### Input JSON:
```json
[
  {
    "user_id": 1,
    "profile": {
      "name": "John Doe",
      "contact": {
        "email": "john@email.com"
      }
    },
    "metadata": {
      "created_date": "2023-01-15",
      "is_active": true
    }
  }
]
```

### Generated CREATE TABLE:
```sql
USE [MyDatabase];
GO

CREATE TABLE [dbo].[users] (
    [user_id] INT IDENTITY NOT NULL,
    [user_name] NVARCHAR(50) NOT NULL,
    [email] NVARCHAR(255) NOT NULL,
    [created_date] DATETIME NOT NULL,
    [is_active] BIT NOT NULL,
    CONSTRAINT PK_users PRIMARY KEY ([user_id])
);
```

### Generated INSERT statements:
```sql
USE [MyDatabase];
GO

INSERT INTO [dbo].[users] ([user_name], [email], [created_date], [is_active])
VALUES
    ('John Doe', 'john@email.com', '2023-01-15', 1),
    ('Jane Smith', 'jane@email.com', '2023-01-16', 0);
GO
```

---

## 🏗️ Project Structure

```
sql-builder/
├── 📄 sqlbuilder.py          # Main application with optimized processing
├── ⚙️ config_manager.py      # Configuration and settings management
├── 🎨 sqlbuilder_icon.ico    # Application icon
├── 📚 README.md              # This comprehensive guide
├── 📋 requirements.txt       # Python dependencies
└── 🧪 tests/                 # Test files (coming soon!)
```

## 🔍 Technical Details

### Data Processing Pipeline
1. **File Analysis**: Automatic delimiter detection and file size estimation
2. **Smart Caching**: Efficient data loading with sample-based type inference
3. **Type Inference**: Statistical analysis of data patterns with regex optimization
4. **UI Generation**: Dynamic interface based on data structure
5. **Script Generation**: Optimized SQL output with chunked processing
6. **Logging & Validation**: Comprehensive audit trail with data validation

### Performance Optimizations
- **Large File Handling**: Configurable MB-based thresholds with automatic chunked processing
- **Memory Management**: Efficient caching and garbage collection
- **Background Processing**: Multi-threaded operations for UI responsiveness
- **Progressive Loading**: Incremental data loading with user feedback
- **JSON Processing**: Intelligent nested object flattening and array handling

### Settings Management
- **Tabbed Interface**: Configuration, Data Processing, SQL Generation, and Logging tabs
- **Persistent Storage**: JSON-based configuration with platform-specific directories
- **Validation**: Input validation for all numeric settings
- **Real-time Updates**: Immediate application of setting changes

### Logging System
- **Thread-Safe**: Handles background script generation with proper synchronization
- **Flexible Storage**: Custom directories or automatic placement with SQL scripts
- **Comprehensive Coverage**: Every operation detail captured for audit purposes
- **Data Validation**: Automatic verification of row counts between source and processed data
- **Error Handling**: Proper logging of both successful and failed operations

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**⭐ If this project helped you, please consider giving it a star! ⭐**

*Developed by [Jack Worthen](https://github.com/jackworthen)*