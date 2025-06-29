# 🚀 SQL Table Builder Pro

**🎯 Transform your data files into SQL magic with just a few clicks!**

---

## 🌟 What is SQL Table Builder Pro?

SQL Table Builder Pro is a powerful, user-friendly desktop application that transforms your messy data files into pristine SQL scripts faster than you can say "SELECT * FROM awesome_table". Whether you're dealing with tiny datasets or massive files, this tool has got your back!

## 🎮 Features That'll Make You Smile

### 🧠 **Smart Data Type Inference**
- Automatically detects data types from your files with statistical sampling
- Supports INT, FLOAT, DATETIME, VARCHAR, BIT, and more
- Intelligent type reset for data-driven columns only
- Customizable type inference with configurable sample percentages

### ⚡ **Lightning-Fast Performance**
- **Chunked Processing**: Handle files with millions of rows without breaking a sweat
- **Intelligent Caching**: Loads data once, uses it everywhere
- **Progressive Loading**: See your data while it's still loading
- **Background Threading**: UI stays responsive during heavy operations

### 🎨 **Beautiful & Intuitive Interface**
- Clean, modern GUI with light blue theme that's easy on the eyes
- Enhanced data preview with alternating row colors and smart column sizing
- Real-time progress dialogs for long operations
- Responsive design that adapts to your data

### 🛠️ **Powerful Customization**
- **Dynamic Primary Key Support**: INT IDENTITY and UNIQUEIDENTIFIER options appear automatically in dropdowns when primary key is selected
- **Column Naming Conventions**: CamelCase, snake_case, UPPERCASE, lowercase transformations
- **Flexible Column Management**: Add custom columns with independent type selection
- **Smart Type Reset**: Reset only original data-driven column types, preserve manual additions
- **Batch INSERT Statements**: Configurable batch sizes for optimal performance
- **Schema & Database Support**: Full SQL Server compatibility

### 📁 **Advanced File Handling**
- **Multi-format Support**: CSV, TSV, and custom delimiter files (|, ;, :, ^, and more!)
- **Auto-delimiter Detection**: Intelligent detection of file delimiters
- **Large File Optimization**: Automatic handling for 50,000+ row files
- **Configurable Preview**: Adjustable preview percentages for data inspection
- **Memory Efficient**: Optimized for minimal memory footprint with chunked processing

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
   - Click "Browse" and select your CSV/TXT file
   - Watch as the delimiter is automatically detected
   - Preview your data with adjustable sample percentage (1-100%)

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

---

## 🔧 Advanced Features

### 🎛️ Configuration Options

| Feature | Description |
|---------|-------------|
| **Dynamic Primary Keys** | INT IDENTITY and UNIQUEIDENTIFIER automatically available when PK is selected |
| **Batch INSERT** | Configurable batch sizes (default: 500 rows) |
| **Large File Mode** | Automatic optimization for 50,000+ row files |
| **Smart Type Inference** | Statistical sampling with customizable percentage |
| **TRUNCATE Option** | Optional table truncation before INSERT |
| **Multi-threading** | Background processing for smooth UI experience |
| **Intelligent Reset** | Reset only original column types, preserve manual additions |

### 🎨 Column Management

#### Naming Conventions
Transform your column names instantly:

```
Original: "User Name"
CamelCase: UserName
snake_case: user_name
UPPERCASE: USERNAME
lowercase: username
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

---

## 📝 Example Usage

### Input CSV:
```csv
user_id,user_name,email,created_date,is_active
1,John Doe,john@email.com,2023-01-15,true
2,Jane Smith,jane@email.com,2023-01-16,false
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
3. **Type Inference**: Statistical analysis of data patterns
4. **UI Generation**: Dynamic interface based on data structure
5. **Script Generation**: Optimized SQL output with chunked processing

### Performance Optimizations
- **Large File Handling**: Automatic switching to chunked processing for 50,000+ rows
- **Memory Management**: Efficient caching and garbage collection
- **Background Processing**: Multi-threaded operations for UI responsiveness
- **Progressive Loading**: Incremental data loading with user feedback

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**⭐ If this project helped you, please consider giving it a star! ⭐**

*Developed by Jack Worthen [Jack Worthen](https://github.com/jackworthen)*
