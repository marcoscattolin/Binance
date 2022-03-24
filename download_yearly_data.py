import datetime
from helpers.download import download_all_tickers
from helpers.download_spark import get_spark_session, spark_download_price_hist
import pyspark.sql.functions as func
import logging
import sys

if __name__ == '__main__':
    
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    # get year
    year = int(sys.argv[1])
    #year = 2022
    
    # definisci start date e end date da scaricare
    start_dt  = datetime.date(year, 1, 1)
    end_dt    = datetime.date(year, 12, 31)
    
    # init spark session
    spark = get_spark_session()
    
    # download dei tickers
    logging.info(f'Downloading prices for year {year}')
    tickers = download_all_tickers()
    tickers = spark.createDataFrame(tickers)

    # inserisci date di riferimento nel dataframe 
    # necessario per parallelizzare
    tickers = (
        tickers
        .withColumn('start_str', func.lit(start_dt).cast('string'))
        .withColumn('end_str', func.lit(end_dt).cast('string'))
    )
    
    # download dei prezzi in contesto spark, circa 6 minuti
    schema = 'Symbol string, Timestamp timestamp, Open float, High float, Low float, Close float, Volume float'
    prices = tickers.groupBy('symbol', 'start_str', 'end_str').applyInPandas(spark_download_price_hist, schema=schema)
    prices.repartition(16).write.parquet(f'./datasets/prices_{start_dt}_{end_dt}.parquet', mode = 'overwrite')
    
    logging.info('Done!')
