@echo off
call C:\anaconda\condabin\activate pyspark
cd C:\Users\marco.scattolin\Documents\00_Analytics\03_PersonalProjects\Binance
call python get_trading_recommentations.py
