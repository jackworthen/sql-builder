# SQL Table Builder Pro

A powerful, user-friendly desktop application that automatically generates SQL CREATE TABLE and INSERT statements from CSV and other delimited data files. Built with Python and Tkinter, this tool streamlines the process of converting data files into SQL scripts for database operations.

## ✨ Features

### Core Functionality
- **Multi-format Support**: Works with CSV, TXT, DAT, and other delimited files
- **Intelligent Type Inference**: Automatically detects appropriate SQL data types from your data
- **Flexible Schema Configuration**: Customize database, schema, and table names
- **Primary Key Support**: Add INT IDENTITY or UNIQUEIDENTIFIER primary keys
- **Column Customization**: Rename columns and adjust data types as needed

### Advanced Capabilities
- **Large File Optimization**: Efficiently handles large datasets with chunked processing
- **Batch INSERT Generation**: Create batched INSERT statements for better performance
- **Data Preview**: Preview your data with adjustable sample percentages
- **Smart Caching**: Intelligent file caching to avoid redundant processing
- **Column Formatting**: Apply naming conventions (CamelCase, snake_case, etc.)
- **Progress Tracking**: Real-time progress dialogs for long operations

### SQL Generation Options
- CREATE TABLE statements with proper constraints
- INSERT INTO statements (single or batched)
- TRUNCATE TABLE options for data replacement
- Configurable batch sizes for optimal performance
- Support for NULL/NOT NULL constraints

## 🚀 Getting Started

### Prerequisites
- Python 3.7 or higher
- tkinter (usually included with Python)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/jackworthen/sql-table-builder-pro.git
   cd sql-table-builder-pro
   ```

2. **Run the application:**
   ```bash
   python sqlbuilder.py
   ```

### Quick Start

1. **Select Your File**: Click "Browse" to select your CSV or delimited data file
2. **Configure Delimiter**: The application will auto-detect the delimiter, or set it manually
3. **Preview Data**: Use the "Show" button to preview your data
4. **Configure Table**: Set database, schema, and table names
5. **Customize Columns**: Adjust column names and data types as needed
6. **Generate Scripts**: Choose CREATE and/or INSERT options and save your SQL files

## 📊 Screenshots

### File Selection Screen
The main interface for selecting and previewing your data files.

### Column Configuration Screen
Detailed configuration of table structure, data types, and constraints.

## 🛠️ Configuration

### Settings Panel
Access comprehensive settings through **Edit > Settings**:

#### Database Configuration
- Default database name
- Default schema (default: "dbo")

#### Data Processing
- Maximum additional columns
- Default preview percentage
- Sample percentage for analysis
- Automatic type inference toggle

#### SQL Generation
- Include CREATE TABLE statements
- Include INSERT statements
- Batch INSERT options

#### Performance
- Insert batch size (default: 5000 rows)
- Optimized processing for large files

## 📁 File Structure

```
sql-table-builder-pro/
├── sqlbuilder.py          # Main application file
├── config_manager.py      # Configuration management
├── sqlbuilder_icon.ico    # Application icon
├── help.html             # Help documentation
└── README.md             # This file
```

## 🔧 Technical Details

### Architecture
- **DataCache Class**: Efficient caching system for large file handling
- **OptimizedTypeInferrer**: Statistical sampling for accurate type detection
- **ProgressWindow**: User-friendly progress tracking for long operations
- **ThreadPoolExecutor**: Background processing for responsive UI

### Performance Optimizations
- **Chunked Reading**: Process large files in manageable chunks
- **Smart Sampling**: Use statistical sampling for type inference
- **Regex Compilation**: Pre-compiled patterns for faster data analysis
- **Memory Management**: Efficient memory usage for large datasets

### Supported Data Types
- BIGINT, INT, SMALLINT, TINYINT
- FLOAT, REAL, DECIMAL, NUMERIC
- NVARCHAR (various sizes), VARCHAR, CHAR
- DATETIME, DATETIME2, DATE, TIME
- BIT (for boolean values)
- UNIQUEIDENTIFIER
- And more...

## 🎯 Use Cases

### Database Migration
Convert legacy data files to modern SQL database formats.

### Data Import
Quickly generate INSERT statements for bulk data loading.

### Schema Generation
Create table schemas from sample data files.

### Development & Testing
Generate test data scripts for development environments.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Setup
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🐛 Bug Reports & Feature Requests

Please use the [GitHub Issues](https://github.com/jackworthen/sql-table-builder-pro/issues) page to report bugs or request new features.

## 📞 Support

If you encounter any issues or need help:
- Check the built-in help documentation (**Help > Documentation**)
- Review the [Issues](https://github.com/jackworthen/sql-table-builder-pro/issues) page
- Create a new issue with detailed information about your problem

## 🎉 Acknowledgments

- Built with Python and Tkinter for cross-platform compatibility
- Inspired by the need for efficient data-to-SQL conversion tools
- Thanks to the Python community for excellent libraries and documentation

## 📈 Version History

### v1.5.0 (2025-05-27)
- ✅ Intelligent file caching system
- ✅ Chunked processing for large files
- ✅ Optimized type inference with statistical sampling
- ✅ Progressive loading with progress dialogs
- ✅ Enhanced performance and memory management

---

**Developed by Jack Worthen** | [Report Issues](https://github.com/jackworthen/sql-table-builder-pro/issues) | [Contribute](https://github.com/jackworthen/sql-table-builder-pro/pulls)