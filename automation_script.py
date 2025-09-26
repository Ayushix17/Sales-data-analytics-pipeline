# Automated ETL Pipeline & Dashboard Deployment Script
# This script automates the entire sales data pipeline process

import os
import sys
import time
import schedule
import subprocess
import json
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from pathlib import Path

# Import our custom ETL class
from sales_etl_pipeline import SalesDataETL

class SalesAutomationManager:
    """
    Comprehensive automation manager for sales data pipeline
    """
    
    def __init__(self, config_file='config.json'):
        self.config = self.load_config(config_file)
        self.setup_logging()
        self.etl = SalesDataETL(self.config.get('database_path', 'sales_data.db'))
        
    def load_config(self, config_file):
        """Load configuration from JSON file"""
        default_config = {
            "database_path": "sales_data.db",
            "backup_path": "backups/",
            "log_path": "logs/",
            "email_alerts": {
                "enabled": False,
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "sender_email": "",
                "sender_password": "",
                "recipients": []
            },
            "data_sources": {
                "crm_api_url": "",
                "erp_api_url": "",
                "pos_api_url": ""
            },
            "quality_thresholds": {
                "min_daily_transactions": 100,
                "max_transaction_amount": 10000,
                "data_freshness_hours": 24
            },
            "schedule": {
                "etl_frequency": "hourly",  # hourly, daily, weekly
                "backup_frequency": "daily",
                "report_frequency": "weekly"
            }
        }
        
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
            else:
                # Create default config file
                with open(config_file, 'w') as f:
                    json.dump(default_config, f, indent=2)
                    
        except Exception as e:
            print(f"Error loading config: {e}. Using defaults.")
            
        return default_config
    
    def setup_logging(self):
        """Setup comprehensive logging"""
        log_dir = Path(self.config['log_path'])
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"sales_automation_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger('SalesAutomation')
    
    def create_backup(self):
        """Create database backup"""
        try:
            backup_dir = Path(self.config['backup_path'])
            backup_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = backup_dir / f"sales_backup_{timestamp}.db"
            
            # Copy database file
            import shutil
            shutil.copy2(self.config['database_path'], backup_file)
            
            self.logger.info(f"Database backup created: {backup_file}")
            
            # Cleanup old backups (keep last 7 days)
            cutoff_date = datetime.now() - timedelta(days=7)
            for backup in backup_dir.glob("sales_backup_*.db"):
                if backup.stat().st_mtime < cutoff_date.timestamp():
                    backup.unlink()
                    self.logger.info(f"Removed old backup: {backup}")
                    
        except Exception as e:
            self.logger.error(f"Backup failed: {str(e)}")
            self.send_alert("Backup Failed", f"Database backup failed: {str(e)}")
    
    def validate_data_quality(self):
        """Perform data quality checks"""
        quality_issues = []
        
        try:
            with sqlite3.connect(self.config['database_path']) as conn:
                # Check transaction count
                today = datetime.now().strftime('%Y-%m-%d')
                result = pd.read_sql(
                    "SELECT COUNT(*) as count FROM sales_transactions WHERE DATE(transaction_date) = ?",
                    conn, params=[today]
                )
                
                daily_count = result.iloc[0]['count']
                min_threshold = self.config['quality_thresholds']['min_daily_transactions']
                
                if daily_count < min_threshold:
                    issue = f"Low transaction count today: {daily_count} (expected: >{min_threshold})"
                    quality_issues.append(issue)
                    self.logger.warning(issue)
                
                # Check for anomalous transaction amounts
                result = pd.read_sql(
                    "SELECT MAX(total_amount) as max_amount FROM sales_transactions WHERE DATE(transaction_date) = ?",
                    conn, params=[today]
                )
                
                max_amount = result.iloc[0]['max_amount'] or 0
                max_threshold = self.config['quality_thresholds']['max_transaction_amount']
                
                if max_amount > max_threshold:
                    issue = f"Unusually high transaction detected: ${max_amount:,.2f}"
                    quality_issues.append(issue)
                    self.logger.warning(issue)
                
                # Check data freshness
                result = pd.read_sql(
                    "SELECT MAX(transaction_date) as latest_date FROM sales_transactions",
                    conn
                )
                
                if result.iloc[0]['latest_date']:
                    latest_date = datetime.strptime(result.iloc[0]['latest_date'], '%Y-%m-%d')
                    hours_old = (datetime.now() - latest_date).total_seconds() / 3600
                    freshness_threshold = self.config['quality_thresholds']['data_freshness_hours']
                    
                    if hours_old > freshness_threshold:
                        issue = f"Data is stale: {hours_old:.1f} hours old (threshold: {freshness_threshold}h)"
                        quality_issues.append(issue)
                        self.logger.warning(issue)
                
        except Exception as e:
            issue = f"Data quality check failed: {str(e)}"
            quality_issues.append(issue)
            self.logger.error(issue)
        
        return quality_issues
    
    def generate_performance_report(self):
        """Generate automated performance report"""
        try:
            report_data = self.etl.generate_kpi_report()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Create HTML report
            html_report = f"""
            <html>
            <head><title>Sales Performance Report - {datetime.now().strftime('%Y-%m-%d')}</title></head>
            <body style="font-family: Arial, sans-serif; margin: 40px;">
                <h1>📊 Sales Performance Report</h1>
                <p><strong>Generated:</strong> {timestamp}</p>
                
                <h2>📈 Revenue Trends</h2>
                {report_data['revenue_trends'].to_html(index=False, table_id='revenue-table')}
                
                <h2>🏆 Top Products</h2>
                {report_data['top_products'].head(10).to_html(index=False, table_id='products-table')}
                
                <h2>👥 Customer Segments</h2>
                {report_data['customer_segments'].to_html(index=False, table_id='segments-table')}
                
                <style>
                    table {{ border