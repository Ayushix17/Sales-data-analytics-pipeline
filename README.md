# Sales-data-analytics-pipeline
🚀 Sales Data ETL & Automation Dashboard
📌 Project Overview

This project implements a complete data pipeline for sales data, covering ETL (Extract, Transform, Load), automation scripts, and a Power BI dashboard for visualization.

The pipeline extracts sales, customer, and product data, applies cleaning and transformations, loads it into a database, and automates backups, quality checks, and KPI reporting. A Power BI dashboard connects to this data for interactive business insights.

🏗️ Architecture

ETL Pipeline (sales_etl_pipeline.py)

Extracts sample or API-based sales, customer, and product data

Cleans, validates, and enriches data (duplicates removal, missing values handling, metrics calculation)

Loads transformed data into a SQLite database

Automation Script (automation_script.py)

Automates scheduled ETL runs (hourly/daily/weekly)

Performs data quality checks (transaction thresholds, anomalies, freshness)

Generates performance reports (HTML/email)

Creates automated database backups with retention policy

Power BI Dashboard (final_dashboard.pbix)

Connects directly to the ETL-generated SQLite database

Visualizes key KPIs: revenue trends, top products, customer segments

Provides interactive insights for stakeholders

⚙️ Tech Stack

Programming: Python (pandas, numpy, sqlite3, SQLAlchemy, logging)

Database: SQLite

Automation: schedule, logging, email alerts (SMTP)

Visualization: Power BI (interactive dashboard)

Deployment: Configurable JSON-based automation settings

📊 Dashboard Insights

The Power BI dashboard highlights:

📈 Revenue Trends (monthly sales, transaction volumes, average order value)

🏆 Top Products by revenue and units sold

👥 Customer Segments with total and average purchase values

🌍 Regional & Sales Rep Analysis (if connected to extended dataset)

🚀 How to Run

Clone the repo:

git clone https://github.com/your-username/sales-data-etl-dashboard.git
cd sales-data-etl-dashboard


Install dependencies:

pip install -r requirements.txt


Run ETL pipeline manually:

python sales_etl_pipeline.py


Run automation script:

python automation_script.py


Open the Power BI dashboard (final_dashboard.pbix) and connect it to the generated sales_data.db.

📧 Automation Features

Daily/weekly backup rotation (keeps last 7 days)

Email alerts on ETL failures or data quality issues

Logging system with both file and console output

🌟 Key Achievements

End-to-end ETL pipeline implementation with data validation

Fully automated workflow for data refresh, quality, and reporting

Interactive Power BI dashboard delivering actionable business insights

Scalable design ready for API integrations (CRM, ERP, POS systems)

📂 Project Structure
📦 sales-data-etl-dashboard
 ┣ 📜 sales_etl_pipeline.py     # ETL pipeline (Extract, Transform, Load)
 ┣ 📜 automation_script.py      # Automation manager for ETL, backups, reports
 ┣ 📊 final_dashboard.pbix      # Power BI dashboard
 ┣ 📜 requirements.txt          # Python dependencies
 ┣ 📜 config.json               # Configurations (API URLs, thresholds, schedule)
 ┗ 📜 README.md                 # Project documentation
