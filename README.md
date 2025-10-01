# Largest Banks ETL Project

A simple ETL (Extract, Transform, Load) pipeline built in Python to process the *List of Largest Banks by Market Capitalization* from Wikipedia.

## 📌 Project Overview
This project extracts bank data from Wikipedia (archived snapshot), transforms it by adding currency conversions using exchange rates from a CSV file, and loads the results into both:
- A CSV file (`Largest_banks_data.csv`)
- An SQLite database (`Banks.db`)

## ⚙️ Technologies Used
- Python 3.11
- Pandas
- NumPy
- BeautifulSoup4
- SQLite3

## 🚀 Steps in the Pipeline
1. **Extract**
   - Scrape the "Largest Banks by Market Capitalization" table from Wikipedia.
   - Clean the data and create a Pandas DataFrame.

2. **Transform**
   - Read exchange rates from a CSV file.
   - Convert Market Cap (USD) to GBP, EUR, and INR.
   - Round to 2 decimal places.

3. **Load**
   - Save the transformed data into a CSV file.
   - Load it into an SQLite database (`Largest_banks` table).

4. **Query**
   - Run SQL queries on the database, including:
     - View entire table
     - Average Market Cap in GBP
     - Top 5 banks by Market Cap

## ▶️ How to Run
1. Install dependencies:
   ```bash
   python -m pip install pandas numpy beautifulsoup4 requests
1. Download the exchange rate CSV:
    
    ```bash
    wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
    
    ```
    
2. Run the script:
    
    ```bash
    python3.11 banks_project.py
    
    ```
## 📂 Output

- `Largest_banks_data.csv` — transformed dataset
- `Banks.db` — SQLite database with the `Largest_banks` table
- `code_log.txt` — log file with timestamps of each ETL stage

---

💡 This project was developed as part of a data engineering learning lab to demonstrate a complete ETL pipeline with logging and database integration.
