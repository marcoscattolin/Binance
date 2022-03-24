from binance import Client
import pandas as pd
import yaml

def get_binance_client():
    
    # leggi credenziali
    with open('./config/creds.yml', 'r') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)
        api_key = creds['api_key']
        api_secret = creds['api_secret']

    # connetti
    client = Client(api_key, api_secret)
    
    return client

def download_all_tickers():

    # connetti
    client = get_binance_client()
    
    # scarica lisa dei tickers
    tickers = client.get_all_tickers()
    
    # disconnetti
    client.close_connection()

    return tickers

def download_price_hist(sym, start_str, end_str):
    
    # connetti
    client = get_binance_client()

    # scarica prezzi
    klines = client.get_historical_klines(
            symbol=sym,
            interval=Client.KLINE_INTERVAL_1DAY, 
            start_str=start_str,
            end_str=end_str,
            )
    
    # disconnetti
    client.close_connection()

    # formatta dataframe
    data = pd.DataFrame(klines, columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'Quote_av', 'Trades', 'Tb_base_av', 'Tb_quote_av', 'Ignore'])
    data['Symbol'] = sym

    # se ci sono dati
    if data.shape[0] > 0:
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms')
        data[['Open', 'High', 'Low', 'Close', 'Volume']] = data[['Open', 'High', 'Low', 'Close', 'Volume']].applymap(lambda x: float(x))

    # seleziona colonne
    sel = ['Symbol', 'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    data = data[sel]

    return data
