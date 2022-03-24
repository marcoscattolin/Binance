from telegram import Bot
import yaml

def telegram_printer(title, data, tickers, end_dt):

    # leggi credenziali
    with open('./config/creds.yml', 'r') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)
        token = creds['telegram_token']
        chat_id = creds['telegram_chatid']

    bot = Bot(token)
    
    text = f'{title}\n'
    text += f'(data as of {end_dt})\n'
    text += f'------------------------------\n'
    for k, v in data.items():
        tkr_big = [x  + '*' for x in list(v) if x in tickers['BIG']]
        tkr_big = ', '.join(tkr_big)

        tkr_small = [x for x in list(v) if x in tickers['SMALL']]
        tkr_small = ', '.join(tkr_small)
        
        text += (f'{k} : {tkr_big} // {tkr_small}\n\n')

    try:
        bot.send_message(chat_id=chat_id, text = text)
    except:
        print('Could not send telegram update')