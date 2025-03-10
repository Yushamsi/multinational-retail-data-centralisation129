# Centralized Sales Data System

## Table of Contents
- [Project Description](#project-description)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)
- [License](#license)

## Project Description
### Overview
This project aims to centralize sales data from various sources into a single, accessible database. By consolidating data scattered across multiple platforms, including AWS databases and S3 buckets, we enhance data accessibility and reliability, making it a single source of truth for sales analytics.

### Objectives
- **Data Centralization:** Integrate data from diverse sources to a local PostgreSQL database for centralized access.
- **ETL Processes:** Implement robust ETL (Extract, Transform, Load) processes using custom Python classes to extract, clean, and load data.
- **Data Integrity:** Ensure high data quality by cleaning datasets, including null value checks, data type corrections, and removal of corrupted rows.

### Technologies Used
- **Python:** For scripting ETL processes and data manipulations.
- **Pandas:** Used for data cleaning and transformations.
- **PostgreSQL:** Database for storing centralized data.
- **YAML:** For configuration management.
- **APIs:** To extract data from external sources.
- **Boto & Tabular:** Tools used for data handling and processing, with Boto specifically for accessing AWS S3.

### What I have Learn't
- How to set up a PostgreSQL database and integrate it with Python.
- Techniques for extracting data from various file formats including PDFs, APIs, and CSVs.
- Best practices in data cleaning and preparation for analytical use.

## Installation Instructions

### 1. Clone the Repository:

```bash
git clone [repository-url]
cd [repository-name]

### 2. Environment Setup:
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

### 3. Install Dependencies:
pip install -r requirements.txt

### 4. Database Setup:

Ensure PostgreSQL is installed and running.
Create a new database and configure the connection settings in the configuration file.

## Usage Instructions

### Data Extraction:
Run the data_extraction.py script to extract data from configured sources.
data_extraction.py

### Data Cleaning:
Execute the data_cleaning.py to clean the extracted data.
data_cleaning.py

### Data Loading:
Use the database_utils.py to load cleaned data into PostgreSQL.
database_utils.py

File Structure

project-root/
│
├── data_extraction.py    # Extracts data from various sources
├── data_cleaning.py      # Cleans and prepares data
├── database_utils.py     # Handles database connections and operations
├── requirements.txt      # Project dependencies
└── README.md             # Project documentation

## License
This project is licensed under the MIT License. See the LICENSE file for more details.

