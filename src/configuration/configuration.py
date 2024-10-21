# src/configuration/configuration.py

import os

# PDF data saving path
pdf_output_directory = os.path.join('data', 'pdf')

# CSV data saving path
CSV_DIR = os.path.join('data', 'csv')

# Proxy server 

# proxies = {
#     'https': 'https://202.124.188.98:3128'
# }

# SQL Table name
# SQL_TABLE_NAME = 'Food_Prices'

# # SQL connection string
# CONNECTION_STRING = 'mssql://BGL-DTS33\\MSSQLSERVER1/mydb?driver=ODBC+DRIVER+17+FOR+SQL+SERVER'

# Download URL
pdf_source_url = 'https://www.harti.gov.lk/index.php/en/market-information/data-food-commodities-bulletin'

# Date column
date_col = 'Date'

# MetaData line to search
metadata_line1 = '(Wholesale Prices of Rice & Subsidiary Food Crops)'


