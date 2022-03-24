from pyspark.sql import SparkSession
from helpers.download import download_price_hist

def get_spark_session():

    spark = (
        SparkSession
        .Builder()
        .appName('binance_download')
        .getOrCreate()
        )

    return spark

    
def spark_download_price_hist(key, pdf):
    
    sym = key[0]
    start_str = key[1]
    end_str = key[2]

    data = download_price_hist(sym, start_str, end_str)
    
    return data
