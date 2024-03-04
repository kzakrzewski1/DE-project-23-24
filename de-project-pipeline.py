from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

import re
import pandas as pd
import numpy as np
import requests
import random 
from random import randint
from datetime import date, timedelta, datetime

from prefect import flow, task
from prefect.runtime import flow_run, task_run


#NBP API has a limit of 93 days, so we have to send separate requests and concatenate the results to get full data
@task
def get_exchange_rates(spark):
    exchange_rates = pd.DataFrame({'code': [], "mid": [], "effectiveDate":[] })

    for year in range(2002, 2024):
        dataQ1 = requests.get(f"http://api.nbp.pl/api/exchangerates/tables/a/{year}-01-01/{year}-03-31/")
        dataQ2 = requests.get(f"http://api.nbp.pl/api/exchangerates/tables/a/{year}-04-01/{year}-06-30/")
        dataQ3 = requests.get(f"http://api.nbp.pl/api/exchangerates/tables/a/{year}-07-01/{year}-09-30/")
        dataQ4 = requests.get(f"http://api.nbp.pl/api/exchangerates/tables/a/{year}-10-01/{year}-12-31/")


        dataQ1 = pd.json_normalize(dataQ1.json(), "rates", "effectiveDate")[["code", "mid", "effectiveDate"]]
        dataQ2 = pd.json_normalize(dataQ2.json(), "rates", "effectiveDate")[["code", "mid", "effectiveDate"]]
        dataQ3 = pd.json_normalize(dataQ3.json(), "rates", "effectiveDate")[["code", "mid", "effectiveDate"]]
        dataQ4 = pd.json_normalize(dataQ4.json(), "rates", "effectiveDate")[["code", "mid", "effectiveDate"]]

        df_current_year = pd.concat([dataQ1, dataQ2, dataQ3, dataQ4])
        exchange_rates = pd.concat([exchange_rates, df_current_year])

    #today = date.today()
    #today = re.sub(" (.*)", "", str(today))
    
    #final_data = requests.get(f"http://api.nbp.pl/api/exchangerates/tables/a/2024-01-01/{today}/")
    #final_data = pd.json_normalize(final_data.json(), "rates", "effectiveDate")[["code", "mid", "effectiveDate"]]
    #exchange_rates = pd.concat([exchange_rates, final_data])

    exchange_rates.reset_index(inplace=True, drop=True)
    exchange_rates["effectiveDate"] = pd.to_datetime(exchange_rates["effectiveDate"])
    exchange_rates.columns = ["currency", "exchange_rate", "effective_date"]
    
    exchange_rates_df = spark.createDataFrame(exchange_rates)

    exchange_rates_df = exchange_rates_df.filter(f.col("currency") != "XDR")  #drop the currency of IMF (special drawing rights XDR)

    exchange_rates_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save('s3a://project/exchange_rates')
    
    print("EXCHANGE RATES TABLE: ")
    exchange_rates_df.show()

    return('s3a://project/exchange_rates')



@task
def get_country_codes(spark):
    country_codes = pd.read_csv("de-data/currency_country_alpha3.csv")
    country_codes_df = spark.createDataFrame(country_codes)
    country_codes_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save('s3a://project/country_codes')

    return(country_codes, 's3a://project/country_codes')


@task
def get_gdp(spark, country_codes):
    gdp = pd.read_csv("de-data/imf_gdp.csv", decimal = ',')

    #choose only countries for which we have currency exchange data
    idxs_to_use = []
    for (i, country) in enumerate(gdp["country"]):
        if country in list(country_codes["country"]):
            idxs_to_use.append(i)       
        
    gdp = gdp.loc[idxs_to_use,].replace('no data', np.NaN).drop(gdp.columns[range(1,21)], axis = 1) #delete unneeded data from years before 2000

    for column in gdp.columns[1::]:
        gdp[column] = gdp[column].apply(lambda x: x.replace(",",".")).astype("float")

    gdp_df = spark.createDataFrame(gdp)
    gdp_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save('s3a://project/gdp')
    
    print("GDP TABLE: ")
    gdp_df.show()

    return('s3a://project/gdp')


@task
def get_population(spark, country_codes):
    population = pd.read_csv("de-data/imf_population.csv", decimal = ',')

    #choose only countries for which we have currency exchange data
    idxs_to_use = []
    for (i, country) in enumerate(population["country"]):
        if country in list(country_codes["country"]):
            idxs_to_use.append(i)       
        
    population = population.loc[idxs_to_use,].replace('no data', np.NaN).drop(population.columns[range(1,21)], axis = 1) #delete unneeded data from years before 2000

    for column in population.columns[1::]:
        population[column] = population[column].apply(lambda x: x.replace(",",".")).astype("float")

    population_df = spark.createDataFrame(population)
    population_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save('s3a://project/population')

    print("POPULATION TABLE: ")
    population_df.show()

    return('s3a://project/population')



@task
def get_n_highest_rates(spark, exchange_rates_path, country_codes_path, start_date, end_date, n):
    
    exchange_rates_df = spark.read.format('delta').load(exchange_rates_path)
    exchange_rates_df = exchange_rates_df.filter(f.col('effective_date').between(start_date, end_date))

    country_codes_df = spark.read.format("delta").load(country_codes_path)

    n_highest_exchange = exchange_rates_df.groupby(f.col("currency")).agg(
        f.round(f.mean(f.col("exchange_rate")), 4).alias(
            "mean_exchange_rate")).sort(
                f.col("mean_exchange_rate").desc())
    
    n_highest_exchange = n_highest_exchange.limit(n)
    
    #add information about country
    n_highest_exchange = n_highest_exchange.join(
        country_codes_df, on = n_highest_exchange.currency == country_codes_df.currency_code).sort(
        f.col("mean_exchange_rate").desc()).drop("currency")

    #save
    n_highest_exchange.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save('s3a://project/highest_rates_30_days')

    print("TABLE OF CURRENCIES WITH HIGHEST EXCHANGE RATES: ")
    n_highest_exchange.show()

    return('s3a://project/highest_rates_30_days')



@task
def get_n_highest_gains(spark, exchange_rates_path, country_codes_path, start_date, end_date, n):

    exchange_rates_df = spark.read.format('delta').load(exchange_rates_path)
    exchange_rates_df = exchange_rates_df.filter(f.col('effective_date').between(start_date, end_date))

    country_codes_df = spark.read.format("delta").load(country_codes_path)

    n_most_gaining = exchange_rates_df.groupby(f.col("currency")).agg(
        f.round((100*(f.last(f.col("exchange_rate")) - f.first(f.col("exchange_rate")))/(f.first(f.col("exchange_rate")))), 3).alias(
            "exchange_change_percent"))

    n_most_gaining  = n_most_gaining.sort(f.col("exchange_change_percent").desc())
    n_most_gaining = n_most_gaining.limit(n)
    
    #add information about country
    n_most_gaining = n_most_gaining.join(country_codes_df, on = n_most_gaining.currency == country_codes_df.currency_code).drop("currency")
    n_most_gaining  = n_most_gaining.sort(f.col("exchange_change_percent").desc())

    #save
    n_most_gaining.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save('s3a://project/highest_gains_30_days')

    print("TABLE OF CURRENCIES WITH HIGHEST RELATIVE EXCHANGE RATE GAINS: ")
    n_most_gaining.show()

    return('s3a://project/highest_gains_30_days')



@task
def get_n_highest_volatility(spark, exchange_rates_path, country_codes_path, start_date, end_date, n):

    exchange_rates_df = spark.read.format('delta').load(exchange_rates_path)
    exchange_rates_df = exchange_rates_df.filter(f.col('effective_date').between(start_date, end_date))

    country_codes_df = spark.read.format("delta").load(country_codes_path)

    n_most_variable = exchange_rates_df.groupby(f.col("currency")).agg(f.round(100*f.stddev("exchange_rate")/f.mean("exchange_rate"),3).alias("relative_rate_sd"))
    n_most_variable = n_most_variable.sort(f.col("relative_rate_sd").desc())

    n_most_variable = n_most_variable.limit(n)
    
    #add information about country
    n_most_variable = n_most_variable.join(country_codes_df, on = n_most_variable.currency == country_codes_df.currency_code).drop("currency")
    n_most_variable  = n_most_variable.sort(f.col("relative_rate_sd").desc())

    #save
    n_most_variable.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save('s3a://project/highest_volatility_30_days')

    print("TABLE OF CURRENCIES WITH HIGHEST EXCHANGE RATE VOLATILITIES: ")
    n_most_variable.show()

    return('s3a://project/highest_volatility_30_days')




spark_conf = (
        SparkConf()
        .set("spark.jars.packages", 'org.apache.hadoop:hadoop-client:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.0.0')
    
        .set("spark.driver.memory", "6g")
    
        .set("spark.hadoop.fs.s3a.endpoint", "localhost:9000")
        .set("spark.hadoop.fs.s3a.access.key", "accesskey")
        .set("spark.hadoop.fs.s3a.secret.key", "secretkey" )
        .set("spark.hadoop.fs.s3a.path.style.access", "true") 
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

sc = SparkContext.getOrCreate(spark_conf)
spark = SparkSession(sc)

#main pipeline that loads the data from NBP API and csv files
@flow
def data_pipeline(name = "project-basic-pipeline"):
    exchange_rates_path = get_exchange_rates(spark)
    country_codes, country_codes_path = get_country_codes(spark)
    gdp_path = get_gdp(spark, country_codes)
    population_path = get_population(spark, country_codes)
    
    start_date_str = '02-12-2023'
    end_date_str = '31-12-2023'
    start_date = datetime.strptime(start_date_str, '%d-%m-%Y').date()
    end_date = datetime.strptime(end_date_str, '%d-%m-%Y').date()

    highest_rates_path = get_n_highest_rates(spark, exchange_rates_path, country_codes_path, start_date, end_date, 10)
    highest_gains_path = get_n_highest_gains(spark, exchange_rates_path, country_codes_path, start_date, end_date, 10)
    highest_volatility_path = get_n_highest_volatility(spark, exchange_rates_path, country_codes_path, start_date, end_date, 10)

data_pipeline()

#return the first day without data
@task
def check_first_day(spark, exchange_rates_path):
    exchange_rates_df = exchange_rates_df = spark.read.format('delta').load(exchange_rates_path)
    last_df = exchange_rates_df.select(f.max("effective_date"))

    last_day = str(last_df.collect()[0][0] + timedelta(days=1))
    last_day = re.sub(" (.*)", "", last_day)

    return(last_day)



@flow 
def update_pipeline(name = "project-update-pipeline"):
    first_day = check_first_day(spark, 's3a://project/exchange_rates')
    today = date.today()
    today = re.sub(" (.*)", "", str(today))
    
    new_data= requests.get(f"http://api.nbp.pl/api/exchangerates/tables/a/{first_day}/{today}/")
    if(new_data.status_code == 200):
        new_data = pd.json_normalize(new_data.json(), "rates", "effectiveDate")[["code", "mid", "effectiveDate"]]
        new_data.reset_index(inplace=True, drop=True)
        new_data["effectiveDate"] = pd.to_datetime(new_data["effectiveDate"])
        new_data.columns = ["currency", "exchange_rate", "effective_date"]

        new_data_df = spark.createDataFrame(new_data)
        new_data_df = new_data_df.filter(f.col("currency") != "XDR")  #drop the currency of IMF (special drawing rights XDR)

        new_data_df.show()
        
        #new data gets appended to the exchange rates stream
        new_data_df.write.format('delta').mode('append').save('s3a://project/exchange_rates')

    #we also update the 30-day aggregation tables
    highest_rates_path = get_n_highest_rates(spark, 's3a://project/exchange_rates', 's3a://project/country_codes', date.today() - timedelta(days=30), date.today(), 10)
    highest_gains_path = get_n_highest_gains(spark, 's3a://project/exchange_rates', 's3a://project/country_codes', date.today() - timedelta(days=30), date.today(), 10)
    highest_volatility_path = get_n_highest_volatility(spark, 's3a://project/exchange_rates', 's3a://project/country_codes', date.today() - timedelta(days=30), date.today(), 10)


#run the update pipeline every day at 2am
if __name__ == "__main__":
    update_pipeline.serve(name = "data-update-schedule", cron = "0 2 * * * ")
