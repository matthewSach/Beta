# Beta

# Beta

## Info
- **Author**: Matěj Šach, C4c
- **Contact**: sachmataj@gmaail.com
- **Date**: 16.12. 2024
- **School**: SPŠE Ječná
## Project Description

This project is a console-based application written in Python, primarily focused on processing and analyzing large CSV files. It leverages pandas for data manipulation and the multiprocessing module for efficient, parallel processing of big datasets. The application features a menu-driven interface that allows users to perform various data operations such as data validation, cleaning, profit calculations, trend analysis, and more.

## Features

1. **Parallel Data Processing**:
   - Splits large datasets into chunks and processes them in parallel using the
   - Improves performance when working with massive CSV files.

2. **Data Validation and Cleaning**:
   - Filters out invalid rows based on specific columns and constraints (e.g., negative values, future dates).
   - Ensures that only consistent and accurate data is used for further analysis.

3. **Profit and Trend Analysis**:
   - Compares profits between two countries.
   - Analyzes yearly trends to identify top products and best-selling items.

5. **Region-Based Analysis**:
   - Aggregates total profit by region.
   - Highlights potential market opportunities or underperforming regions.

6. **Multitable Operations**:
   - Perform complex operations involving multiple tables with transactional integrity.

7. **File Copying in Parallel**:
   - Demonstrates the efficient handling of large file operations.

8. **Menu-Driven Interface**:
   - Offers a simple console menu to run specific tasks (e.g., compare_profits, analyze_trends, validate_data).

## File Structure

```
beta-python/
├── main.py                  # Single Python script containing all classes (FileLoader, DataProcessor, MainApp)
├── main_test.py             # Test file for unit testing methods in main.py
├── record.csv               # Sample CSV file used for data analysis
├── profit_analysis_log.txt  # Log file where profit calculations are recorded
├── README.json              # Documentation in JSON format
├── cloc-2.02.exe            # Utility for counting lines of code
└── requirements.txt

```

## Installation and Running Instructions

### Prerequisites
- Python 3.8 or higher
- PIP (Python Package Installer)

---

## Installation Steps
1. Clone/Download the repository to your local machine.
2. Install Dependencies:
        pip install -r requirements.txT

---

## 2. Locate the Dist Folder
1. In the unzipped folder, you should find: main.py
2. Confirm that main.py is in the same directory as record.csv

---

## 3. Run the Application
1. In the same terminal/command prompt, execute:
        python main.py
---

## 4. Interact with the Menu
1. Choose an option by typing the corresponding number (e.g., 1 to compare profits between two countries).
2. Follow the on-screen prompts. Some features will ask for user input (e.g., country names).

### Example Usage - ____IMPORTANT_____

## Compare profits between two countries
1. Prompts user for two country names.
2. Parallel processing sums the Total Profit for each country and compares the results.

## Test Report

### Test Results

1. **Large File Handling**:
   - Successfully processed CSV files of tens or hundreds of thousands of rows within a reasonable time, thanks to parallel chunk division.

2. **Validation & Error Handling**:
   - Missing or invalid columns raise custom exceptions.
   - Data without mandatory fields is removed; numeric and date conversions are tested thoroughly.

3. **Performance Tests**:
   - Parallel approach sped up computations for aggregated results like yearly trends or profit comparisons.


## Sources Used

1. [Python Official Documentation](https://docs.python.org/3/)
2. [Refactoring Guru - Design Patterns](https://refactoring.guru/design-patterns)
3. [Pandas Documentation](https://pandas.pydata.org/docs/)
4. [PowerShell Execution Policies Documentation](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_execution_policies)
6. [Python Exceptions Documentation](https://docs.python.org/3/tutorial/errors.html)
7. [Python OS Module Documentation](https://docs.python.org/3/library/os.html)
5. [ChatGPT](https://chatgpt.com/)
8. [Python Input Validation](https://docs.python.org/3/library/functions.html#input)
9. [DATA] - 2m sales racords - https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/

## Conclusion
This console application showcases how to handle large CSV files efficiently in Python using pandas and parallel processing. Its menu-driven design, robust data validation, and clean code structure make it suitable for educational projects or real-world scenarios involving big data analytics in CSV format.
