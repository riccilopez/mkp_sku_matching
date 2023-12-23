# Databricks notebook source
from datetime import datetime, timedelta

MINS_TIMEOUT = 8
NOTEBOOK     = 'sku_matching_pyspark_cluster'
# Define the scraping DATE
DATE_TODAY   = (datetime.utcnow() - timedelta(hours=6, minutes=0)).strftime('%d-%m-%y')
COUNTRIES    = ['MX', 'CO']

# Perform the execution per each country
for country in COUNTRIES:
    print(f'Processing {country}-{DATE_TODAY}')
    try:
        dbutils.notebook.run(
                        'sku_matching_pyspark_cluster', 
                        timeout_seconds = 60 * MINS_TIMEOUT,
                        arguments = {"country": country, 
                                     "date": DATE_TODAY,
                                     "database_out": "deltadb"})
        print(f'\t- Finished with {country}-{DATE_TODAY}.')
    except Exception as e:
        print(f"{country}-{DATE_TODAY} execution Failed:\n", e)
