import datetime
from helpers.download import download_price_hist
import logging
import pandas as pd
from helpers.signals import get_supertrend, get_trading_recs, signal_printer
from helpers.bot_telegram import telegram_printer

if __name__ == '__main__':
    
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    
    # definisci start date e end date da scaricare
    end_dt  = datetime.date.today() + datetime.timedelta(days=-1)
    start_dt  = end_dt + datetime.timedelta(days=-180)
    
    # tickers raggrupati in categorie
    tickers = {
        'BIG' : ['BTCEUR', 'ETHEUR', 'LUNAEUR', 'ADAEUR', 'BNBEUR', 'SOLEUR',],
        'SMALL' : ['XRPEUR', 'MATICEUR', 'AVAXEUR', 'VETEUR', 'EGLDEUR', 'ENJEUR', 'TRXEUR', 'RUNEEUR', 'WINEUR', 'SHIBEUR'],
    }
    
    # download prices
    logging.info(f'[+] Downloading data until {end_dt}...')
    pdf = []
    for tick in tickers['BIG'] + tickers['SMALL']:
        tmp = download_price_hist(tick, str(start_dt), str(end_dt))
        pdf.append(tmp)
    pdf = pd.concat(pdf)
    logging.info(f'[-] Done')

    # calcola supertrend
    logging.info(f'[+] Updating supertrend_df.csv')
    st_df = pdf.groupby('Symbol').apply(get_supertrend, period = 10, multiplier = 3)
    st_df.to_csv('./qlik/supertrend_df.csv', index = False)
    logging.info(f'[-] Done')
    
    # recupera segnali operativi    
    signals, trends = get_trading_recs(st_df)

    # stampa
    signal_printer('BUY / SELL / HOLD signals', signals, tickers, end_dt)
    signal_printer('UP / DOWN trending signals', trends, tickers, end_dt)

    # invia a telegram
    telegram_printer('BUY / SELL / HOLD signals', signals, tickers, end_dt)
    telegram_printer('UP / DOWN trending signals', trends, tickers, end_dt)
