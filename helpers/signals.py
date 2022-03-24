import numpy as np
import pandas as pd

def get_supertrend(df, period = 10, multiplier=3):

    close = df['Close'].to_numpy()
    high = df['High'].to_numpy()
    low = df['Low'].to_numpy()
    prev_close = df['Close'].shift(1).to_numpy()

    # calcola average true range
    x = np.stack([high-low, prev_close-low, prev_close-high])
    tr = np.abs(x).max(axis = 0)
    atr = pd.Series(tr).rolling(period).mean().to_numpy()

    # calcola upper e lower band
    upper_band = (high+low)/2 + (multiplier * atr)
    lower_band = (high+low)/2 - (multiplier * atr)
    
    # calcola supertrend
    # init up_trend
    in_uptrend = np.ones_like(atr)
    supertrend = np.empty_like(atr)

    for current in range(1, atr.shape[0]):
        previous = current-1

        # aggiorna uptrend
        if close[current] > upper_band[previous]:
            in_uptrend[current] = 1
        elif close[current] < lower_band[previous]:
            in_uptrend[current] = 0
        else:
            in_uptrend[current] = in_uptrend[previous]
        
        # aggiorna lower band
        if (in_uptrend[current] == 1):
            if (lower_band[current] < lower_band[previous]):
                lower_band[current] = lower_band[previous]
            supertrend[current] = lower_band[current]

        # aggiorna upper band
        if (in_uptrend[current] == 0):
            if (upper_band[current] > upper_band[previous]):
                upper_band[current] = upper_band[previous]
            supertrend[current] = upper_band[current]
    
    # fix initial values
    supertrend[:period] = np.nan
    in_uptrend[:period] = np.nan

    # make dataframe
    df['Atr'] = atr
    df['Upper_band'] = upper_band
    df['Lower_band'] = lower_band
    df['In_uptrend'] = in_uptrend
    df['Supertrend'] = supertrend
    
    # extract integer signal (1 : buy, 0 : hold, -1 : sell)
    df['Supertrend_Signal'] = df['In_uptrend'].diff()
    df.iloc[period, df.columns.get_loc('Supertrend_Signal')] = 0
    
    return df



def get_trading_recs(df):

    # filtra ultimi dati
    end_date = df['Timestamp'].dt.date.max()
    mask = df['Timestamp'].dt.date == end_date
    last_df = df[mask].copy()
    

    # segnali buy / sell / hold
    signals = {}
    
    # buy
    mask = (last_df['Supertrend_Signal'] == 1)
    signals['BUY'] = last_df.loc[mask, 'Symbol'].unique()

    # sell
    mask = (last_df['Supertrend_Signal'] == -1)
    signals['SELL'] = last_df.loc[mask, 'Symbol'].unique()

    # hold
    mask = (last_df['Supertrend_Signal'] == 0)
    signals['HOLD'] = last_df.loc[mask, 'Symbol'].unique()



    # up / down trend
    trends = {}
    
    # uptrending
    mask = (last_df['In_uptrend'] == 1)
    trends['UP-Trend'] = last_df.loc[mask, 'Symbol'].unique()

    # downtrending
    mask = (last_df['In_uptrend'] == 0)
    trends['DOWN-Trend'] = last_df.loc[mask, 'Symbol'].unique()

    return signals, trends





def signal_printer(title, data, tickers, end_dt):

    pad_length = 54
    title = f'-- {title}'
    title = title.ljust(pad_length, '-')

    print('', end = '\n\n')
    print(''.ljust(pad_length, '-'))
    print(title)
    print(''.ljust(pad_length, '-'))

    for k, v in data.items():
        tkr_big = [x  + '*' for x in list(v) if x in tickers['BIG']]
        tkr_big = ', '.join(tkr_big)

        tkr_small = [x for x in list(v) if x in tickers['SMALL']]
        tkr_small = ', '.join(tkr_small)
        
        print(f'{k} : {tkr_big} || {tkr_small}')
    print(''.ljust(pad_length, '='), end = '\n\n')

    print('Note: * indicates BIG segment Symbol')
    print(f'Data as of: {end_dt}')

