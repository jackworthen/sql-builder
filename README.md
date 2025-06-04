# 🚀 SQL Table Builder Pro

**🎯 Transform your data files into SQL magic with just a few clicks!**

*Created by [Jack Worthen](https://github.com/jackworthen)*

---

## 🌟 What is SQL Table Builder Pro?

SQL Table Builder Pro is a powerful, user-friendly desktop application that transforms your messy data files into pristine SQL scripts faster than you can say "SELECT * FROM awesome_table". Whether you're dealing with tiny datasets or massive files that make your computer sweat, this tool has got your back!

## 🎮 Features That'll Make You Smile

### 🧠 **Smart Data Type Inference**
- Automatically detects data types from your files
- Supports INT, FLOAT, DATETIME, VARCHAR, and more
- Customizable type inference with statistical sampling

### ⚡ **Lightning-Fast Performance**
- **Chunked Processing**: Handle files with millions of rows without breaking a sweat
- **Intelligent Caching**: Loads data once, uses it everywhere
- **Progressive Loading**: See your data while it's still loading

### 🎨 **Beautiful & Intuitive Interface**
- Clean, modern GUI that doesn't hurt your eyes
- Real-time data preview
- Progress dialogs for long operations
- Light blue theme that's easy on the eyes

### 🛠️ **Powerful Customization**
- **Column Naming Conventions**: CamelCase, snake_case, UPPERCASE, lowercase
- **Primary Key Support**: INT IDENTITY and UNIQUEIDENTIFIER options
- **Batch INSERT Statements**: Configurable batch sizes for optimal performance
- **Schema & Database Support**: Full SQL Server compatibility

### 📁 **File Format Support**
- CSV files
- Tab-delimited files
- Custom delimiters (|, ;, :, ^, and more!)
- Large file optimization (50,000+ rows)

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
   - Preview your data with adjustable sample percentage

2. **⚙️ Configure Your Table**
   - Set database and schema names
   - Let the smart type inference do its magic, or customize manually
   - Choose primary keys and configure options

3. **💾 Generate SQL Scripts**
   - CREATE TABLE statements
   - INSERT INTO statements (with optional batching)
   - Save and use in your favorite SQL environment!

---

## 🔧 Advanced Features

### 🎛️ Configuration Options

| Feature | Description |
|---------|-------------|
| **Batch INSERT** | Configurable batch sizes (default: 500 rows) |
| **Large File Mode** | Automatic optimization for 50,000+ row files |
| **Type Inference** | Statistical sampling with customizable percentage |
| **TRUNCATE Option** | Optional table truncation before INSERT |
| **Multi-threading** | Background processing for smooth UI experience |

### 🎨 Column Naming Conventions

Transform your column names instantly:

```
Original: "User Name"
CamelCase: UserName
snake_case: user_name
UPPERCASE: USERNAME
lowercase: username
```

### ⚡ Performance Optimization

- **Smart Caching**: Files are loaded once and cached for multiple operations
- **Chunked Reading**: Large files are processed in manageable chunks
- **Asynchronous Processing**: UI remains responsive during long operations
- **Memory Efficient**: Optimized for minimal memory footprint

---

## 🤝 Contributing

We love contributions! Here's how you can help make SQL Table Builder Pro even more awesome:

### 🐛 Found a Bug?
1. Check if it's already reported in [Issues](https://github.com/jackworthen/sql-builder/issues)
2. If not, create a new issue with:
   - Clear description
   - Steps to reproduce
   - Your environment details

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
CREATE TABLE [dbo].[users] (
    [user_id] INT NOT NULL,
    [user_name] NVARCHAR(50) NOT NULL,
    [email] NVARCHAR(255) NOT NULL,
    [created_date] DATETIME NOT NULL,
    [is_active] BIT NOT NULL,
    CONSTRAINT PK_users PRIMARY KEY ([user_id])
);
```

### Generated INSERT statements:
```sql
INSERT INTO [dbo].[users] ([user_id], [user_name], [email], [created_date], [is_active])
VALUES
    (1, 'John Doe', 'john@email.com', '2023-01-15', 1),
    (2, 'Jane Smith', 'jane@email.com', '2023-01-16', 0);
```

---

## 🏗️ Project Structure

```
sql-builder/
├── 📄 sqlbuilder.py          # Main application file
├── ⚙️ config_manager.py      # Configuration management
├── 🎨 sqlbuilder_icon.ico    # Application icon
├── 📚 README.md              # This awesome file!
├── 📋 requirements.txt       # Python dependencies
└── 🧪 tests/                 # Test files (coming soon!)

```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**⭐ If this project helped you, please consider giving it a star! ⭐**

*Developed by Jack Worthen [Jack Worthen](https://github.com/jackworthen)*

---

**Happy SQL Building! 🎉**
