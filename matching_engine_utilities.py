# Databricks notebook source
# MAGIC %md
# MAGIC # Product matching 
# MAGIC ### Install the requirements and load the libraries

# COMMAND ----------

# DBTITLE 1,Install the required libraries
# Install the requirements
%pip install -r ./requirements.txt

# COMMAND ----------

import os
import pandas as pd
import configparser
from pathlib import Path
from datetime import datetime, timedelta
# Import the in-house libraries
from modules.file_reader import MktpPricesFileReader, OnlineFileReader
from modules.sku_matcher import get_confidence
from modules.normalize_text import normalize_text

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructField, StringType,\
     FloatType, StructType, DateType, DoubleType

spark.conf.set("spark.sql.shuffle.partitions", "36")

# COMMAND ----------

# DBTITLE 1,Define some constants
# Define the input and output directories
MAIN_PATH = '/dbfs/mnt/webscraping'
MKPT_PRICE_PATH        = f'{MAIN_PATH}/marketplace_prices/'
WEBSCRAPING_INPUT_PATH = f'{MAIN_PATH}/webscraping_files/'
MATCHING_OUPUT_PATH    = './data/matched_parquets/'
# Timezone CDMX (UTC - 6 hours)
date_today = (datetime.utcnow() - timedelta(hours=6, minutes=0)).strftime('%d-%m-%y')

# WIDGETS
# Country widget
dbutils.widgets.dropdown("country", "MX", ["MX", "CO"])
country  = dbutils.widgets.get("country")
# Date widget
dbutils.widgets.text("date", date_today)
date_sf  = dbutils.widgets.get("date")
# Database widget
dbutils.widgets.dropdown("database_out", "deltadb", ["deltadb", "default"])
database = dbutils.widgets.get("database_out")

# Output
print(f'Country: {country}, Date dd-mm-yy: {date_sf}, database: {database}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the `webscraping` container

# COMMAND ----------

# Temporal cell
# mount webscraping container
config = configparser.ConfigParser()
config.read('./credentials/azure_storage_conf.ini')

storage_account_name = config['AzureBlobStorage']['account_name']
storage_account_key  = config['AzureBlobStorage']['account_key']
container_name       = config['AzureBlobStorage']['container_name']

# Mount the container into databricks
mount_path = f"/mnt/{container_name}"
# check if the container is already mounted
if not any(mount.mountPoint == mount_path for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
                   mount_point = mount_path,
                   extra_configs = {
                      f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": 
                      storage_account_key
                    }
                 )
  print(f"/mnt/{container_name} has just been mounted.")
else:
  print(f"Already mounted.")
# Check the mounted objects
# dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the input files
# MAGIC ### ABI Marketplace SKU-Price catalog

# COMMAND ----------

mkp_delta_name = 'mktp_prices_catalog'

# Get the file path by formating the date according to the naming convention
# `country`-`YYYY`-`dd`-`mm` MX-2023-11-10.parquet
date_YDM_formated = datetime.strptime(date_sf, "%d-%m-%y").strftime("%Y-%d-%m")
mktp_prices_path = Path(f'{MKPT_PRICE_PATH}/{country}/{country}-{date_YDM_formated}.parquet')

# Read the parquet file using the `MktpPricesFileReader` class
df_mkpt_prices = MktpPricesFileReader(mktp_prices_path).read_file()

# Check that the given `date` is correct and unique
# if not, raise an error and interrupt the process
continue_falg = False
try:
    assert df_mkpt_prices['date'].nunique() == 1
    assert df_mkpt_prices['date'].unique()[0].strftime("%d-%m-%y") == date_sf
except TypeError as e:
    print(f'Process interrupted ({e}): The provided file was incorrect.')
except AssertionError as e:
    print(f'Process interrupted: The `date` column has an incorrect format.')
else:
    # Convert to pyspark dataframe
    df_mkpt_prices.iteritems = df_mkpt_prices.items
    df_mkpt_prices = spark.createDataFrame(df_mkpt_prices)
    df_mkpt_prices = df_mkpt_prices.withColumn("date", F.col("date").cast(DateType()))
    # display(df_mkpt_prices.show(3))
    continue_falg = True

# Continue
assert continue_falg, 'Stopped do to incorrect execution.'

# Check that the provided `country` and `date` does not already exists in the delta table
df_all_mkp = spark.sql(f"select distinct `country`, `date` from {database}.mktp_prices_catalog")
# Check wheter the country and date already exist
country_exists    = (df_all_mkp.country == country)
date_YMD_formated = datetime.strptime(date_sf, "%d-%m-%y").strftime("%Y-%m-%d")
date_exists       = (df_all_mkp.date == F.lit(date_YMD_formated))
already_exists    = df_all_mkp.filter(country_exists & date_exists).count() > 0

# Append the new data if not exists
if not already_exists:
    print(f"Adding data into the delta table: `{mkp_delta_name}`")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")
    df_mkpt_prices.write.format("delta")\
                  .mode("append").saveAsTable(mkp_delta_name)
else:
    print(f"Data ({country}, {date_YMD_formated}) already exist (`{mkp_delta_name}`).")

# COMMAND ----------

# Get the list of sku and sku_names
# Define the query:
mktp_cat_query = f"""
select 
  distinct pc.country, pc.sku, pc.sku_name 
from {database}.mktp_prices_catalog as pc
inner join deltadb.master_catalog_mkp as mp
    on pc.country = mp.country
    and pc.sku    = mp.sku
where pc.date >= dateadd(day, -15, getdate()) 
      and pc.date <= getdate()
      and pc.country = '{country}';
"""
df_mktp_cat = spark.sql(mktp_cat_query)\
                   .dropDuplicates(['country', 'sku'])

# Decorate the 'normalize_text' function
@F.udf(StringType())
def normalize_text_udf(text):
    return normalize_text(text, encode='ascii')

# Cleaning phase
# Lowercase string fields
df_mktp_cat = df_mktp_cat.withColumn(
                        'mkp_sku_name_clean', 
                        normalize_text_udf(df_mktp_cat['sku_name']))
df_mktp_cat.cache()#.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the web scraping files

# COMMAND ----------

# Path to the web scraping directory
scraped_path = Path(WEBSCRAPING_INPUT_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Already processed competitors
# MAGIC - Identify competitors that have been already processed for the selected date

# COMMAND ----------

# Verify if there is matched data for today, 
# and identify the competitors already loaded
sel_date_matches = f"""
select 
  distinct competitor_name
from {database}.matched_skus_webscraping
where 
  date = '{date_YMD_formated}'
  and country = '{country}'
group by competitor_name
having count(*) > 0
"""
df_comp_matched = spark.sql(sel_date_matches)
# Get the competitors already processed as a list
comps_already_processed = df_comp_matched\
                            .select('competitor_name')\
                            .rdd.map(lambda x: x[0]).collect()
print(comps_already_processed)

# COMMAND ----------

# Date to match (corresponds to a single directory per date)
scraping_date = date_sf # datetime.strptime(date_sf, "%d-%m-%y").strftime("%d%m%Y")
date_path = scraped_path / scraping_date 

# List of .txt files inside the date directory
scraping_files = list(date_path.glob('*.txt'))

# STOP the rest of process if there is no webscraping data
assert len(scraping_files) > 0, f'There are no webscraping files for {date_sf} date.'

# Filter out those competitors already processed using the list
# created in the previous cell
def get_competitor_name(com_path: str) -> str:
    comp_name = com_path.name.split('-')[-1].replace('.txt', '')
    return comp_name
# Update the 'scraping_files' list if some have been already processed
if len(comps_already_processed) > 0:
  scraping_files = [f for f in scraping_files 
  if get_competitor_name(f) not in comps_already_processed]

# Remainded files to process
scraping_files

# COMMAND ----------

# Use the `OnlineFileReader` to load and clean the .txt files
try:
  df_scrap = pd.concat([OnlineFileReader(f).read_file() for f in scraping_files])\
                .reset_index(drop = True)
except ValueError as e:
  raise Exception(f'No webscraping data to process for {date_sf} date.')

# Reset index index for future analaysis
# df_scrap = df_scrap.reset_index(names = 'scrap_id')
print('DataFrame dims.:', df_scrap.shape)

# Convert to SparkDF
# TODO: Move inside the `file_reader.py` module
df_scrap.iteritems = df_scrap.items
schema = StructType([ \
    StructField("type",     StringType(), False), \
    StructField("country",  StringType(), False), \
    StructField("locality", StringType(), False), \
    StructField("date",     DateType(),   False), \
    StructField("competitor_name",     StringType(), True), \
    StructField("competitor_sku_name", StringType(), True), \
    StructField("competitor_price",    DoubleType(), True), \
    StructField("special_price",       DoubleType(), True), \
    StructField("competitor_url",      StringType(), True), \
    StructField("gift_or_extra_prod",  StringType(), True)
  ])
df_scrap = spark.createDataFrame(df_scrap, schema=schema)

# TODO: Check how to manage multiple countries
# por ahora mantengo sólo lo de MX
df_scrap = df_scrap.filter(df_scrap.country == country)

# Text cleaning phase
df_scrap = df_scrap.withColumn('comp_sku_name_clean', 
            normalize_text_udf(df_scrap['competitor_sku_name']))
            
df_scrap.cache()#.groupby('competitor_name').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform the text-cleaning phase of web scraping data

# COMMAND ----------

# Get only the cleaned sku names
df_comp_names = df_scrap[['comp_sku_name_clean']].dropDuplicates()
df_comp_names.cache()#.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SKU matching phase
# MAGIC
# MAGIC ### Number of evaluations to perform
# MAGIC Calculate the number of evaluations to perform:
# MAGIC
# MAGIC $$N_{evals} = m * n$$
# MAGIC where $m$= *number of Marketplace skus* and $n$ = *number of scraped products*

# COMMAND ----------

# Total de evaluaciones
#_n_rows_mkp = df_mktp_cat.count()
#_n_rows_wsp = df_comp_names.count()
#_n_evals =_n_rows_mkp * _n_rows_wsp
#print(f'[{_n_rows_mkp:,} mkp skus] * [{_n_rows_wsp:,} web scraping skus] ')
#print(f'= {_n_evals:,} evaluations')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform the matching phase

# COMMAND ----------

# MAGIC %md
# MAGIC Define a function to perform the matching phase and keep the best match between a given pair of competitor and marketplace skus.

# COMMAND ----------

# Define the UDF for confidence matching 
@F.udf(DoubleType())
def get_confidence_udf(mktp_sku_clean_name, comp_sku_clean_name):
    # Replace 'get_confidence'
    conf = get_confidence(mktp_sku_clean_name, comp_sku_clean_name)
    return conf

# COMMAND ----------

# Perform a cross join to obtain all pair combinations of mkp and comp products
# Broadcast the smallest table to enhance performance. 
# Keep only relevant columns to avoid memory overload
cj_df = F.broadcast(df_mktp_cat.select(["sku", "mkp_sku_name_clean"]))\
        .crossJoin(df_comp_names.select(["comp_sku_name_clean"]))

# Perform the pair-wise evaluation using the 'get_confidence' function
cj_df = cj_df.withColumn("confidence", 
                 get_confidence_udf(F.col('mkp_sku_name_clean'), 
                                    F.col('comp_sku_name_clean')))

# Now get the best mktp match for each competitor sku
# Returns the following structure:
# sku|mkp_sku_name_clean|scrap_id|comp_sku_name_clean|confidence|
w = Window.partitionBy('comp_sku_name_clean')
match_df = cj_df\
        .withColumn('best_conf', F.max('confidence').over(w))\
        .where(F.col('confidence') == F.col('best_conf'))\
        .drop('best_conf')\
        .dropDuplicates(['comp_sku_name_clean']) # Keep only one occurrence 
# Keep only relevant columns
match_df = match_df.select(['comp_sku_name_clean', 'sku', 'confidence'])
match_df.cache()#.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Format the output and save to `.parquet`
# MAGIC
# MAGIC 1. Keep only relevant matches
# MAGIC 3. Keep only relevant columns

# COMMAND ----------

# Confidence interval
conf_thr = 0.4
save_mode = "append"

# Merge the df_scrap with the matching mktp_sku using 
df_matched = df_scrap.join(match_df, ['comp_sku_name_clean'], "left")\
                     .filter(F.col('confidence') >= conf_thr)\
                     .drop('comp_sku_name_clean')\
                     .orderBy(F.col('confidence').desc())

# Save to cache
df_matched.cache()
df_matched.write.format("delta")\
          .mode(save_mode)\
          .saveAsTable(f"{database}.matched_skus_webscraping")
# option("overwriteSchema", "true") -- activate when "overwrite" mode

# COMMAND ----------

#print(df_matched.count())
#df_matched.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount the container

# COMMAND ----------

#dbutils.fs.unmount("/mnt/webscraping")

# COMMAND ----------


