# Vendor-Performance-Analysis-EDA

End‑to‑end analytics project using SQL & Python to evaluate vendor efficiency and drive real business impact

This project analyzes vendor performance through key metrics like delivery timeliness, product quality, and cost—using SQL and Python from raw CSV ingestion to database storage and audit logs.


🧭 Workflow Breakdown

1. Data Extraction

Raw CSVs ingested into a staging area using Python or SQL bulk load mechanisms.

2. ETL & Logging

Processed and cleaned via SQL transforms and Python (Pandas, NumPy).

Each load event is recorded in a log file capturing:

-Timestamp

-Rows processed

-Data quality checks (duplicates/missing values)

-Error details (if any)

3. Database Loading
   
Clean datasets saved into the production schema with SQL scripts (e.g. BULK INSERT, DataFrame .to_sql()).

4. Exploratory Data Analysis (Python)

Correlation analysis, clustering vendors based on performance metrics

Identification of outliers in delivery or quality scores

📌 Key Insights & Business Value

Top‑performing suppliers identified with strong delivery and quality track records

Underperformers flagged based on delays, defects, or high cost

Vendor scoring system developed to drive strategic procurement decisions

Log audit trail ensures transparency and reliability of data ingestion

Data‑driven insights enabling vendor prioritization and negotiation strategies
