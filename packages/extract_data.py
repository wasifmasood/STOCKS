import pandas as pd
import pandas_datareader.data as web
import numpy as np
import pickle
import requests
import shutil
import urllib.request as request
import os
import arrow
import datetime
from datetime import date
import string
import bs4 as bs
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import platform
from alpha_vantage.timeseries import TimeSeries

file_name_symbols = 'D:/Users/masoodw/ML_FINANCE/data/symbols/all_symbols.csv'
main_path = 'D:/Users/masoodw/ML_FINANCE/data/quotes/'


######################################################################################################################
def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds())


######################################################################################################################
################################### World Famous Indices & Symbols within ############################################
######################################################################################################################
def get_famous_symbols(fresh=False):
    
    file_indices_symbols = 'D:/Users/masoodw/ML_FINANCE/data/symbols/famous_symbols.csv'
    
    if fresh==False:
        if platform.system() == 'Windows':
            c_time = os.path.getctime(file_indices_symbols)
            file_creation_dt = datetime.datetime.fromtimestamp(c_time).strftime('%Y-%m-%d')
            print(' File last modifed at:' + file_creation_dt)
    
        return pd.read_csv(file_indices_symbols, sep='|')


    df_dic = {'Indices': ['FTSE 100', 'S&P 500', 'NASDAQ 100', 'NIKKEI 225', 'DAX',
                            'CAC 40', 'FTSE MIB', ' IBEX 35', 'ASX 200', 'MOEX'
                         ],
              'URLS': ['https://tradingeconomics.com/united-kingdom/stock-market',
                       'https://tradingeconomics.com/spx:ind',
                       'https://tradingeconomics.com/ndx:ind',
                       'https://tradingeconomics.com/japan/stock-market',
                       'https://tradingeconomics.com/germany/stock-market',
                       'https://tradingeconomics.com/france/stock-market',
                       'https://tradingeconomics.com/italy/stock-market',
                       'https://tradingeconomics.com/spain/stock-market',
                       'https://tradingeconomics.com/australia/stock-market',
                       'https://tradingeconomics.com/russia/stock-market'
                      ],
              'TableClass': ['table table-hover sortable-theme-minimal table-heatmap',
                             'table table-hover sortable-theme-minimal',             
                             'table table-hover sortable-theme-minimal',             
                             'table table-hover sortable-theme-minimal table-heatmap',
                             'table table-hover sortable-theme-minimal table-heatmap',
                             'table table-hover sortable-theme-minimal table-heatmap',
                             'table table-hover sortable-theme-minimal table-heatmap',
                             'table table-hover sortable-theme-minimal table-heatmap',
                             'table table-hover sortable-theme-minimal table-heatmap',
                             'table table-hover sortable-theme-minimal table-heatmap'
                            ],
              'YahooPrefix': ['.L', '', '', '.T', '.DE', '.PA', '.MI', '.MC', '.AX', '.ME']
             }
    df_access_info = pd.DataFrame(data=df_dic)

    ticker_symbol = []
    ticker_name = []    
    index_name = []    

    session = requests.Session()
    retry = Retry(connect=10, backoff_factor=0.1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)        

    
    for index, row in df_access_info.iterrows():
        res = session.get(row['URLS'])    
        soup = bs.BeautifulSoup(res.text, 'lxml')
        table = soup.find('table', {'class': row['TableClass']})
        for tr in table.findAll('tr')[1:]:
            symbol = tr.findAll('td')[0].text.rstrip().lstrip().replace("/", "")+ row['YahooPrefix']
            name = tr.findAll('td')[1].text.rstrip().lstrip().replace("/", "")
            ticker_symbol.append(symbol)
            ticker_name.append(name)
            index_name.append(row['Indices'])


    ticker_symbol = list(map(lambda s: s.strip(), ticker_symbol))
    ticker_name = list(map(lambda s: s.strip(), ticker_name))
    index_name = list(map(lambda s: s.strip(), index_name))
    symbols_df = pd.DataFrame(list(zip(ticker_symbol, ticker_name, index_name)), columns =['Symbol', 'Name', 'Index'])
    
    if(fresh):
        symbols_df.to_csv(file_indices_symbols, sep='|', index=False)

    
    return symbols_df



######################################################################################################################
########################################## Asset Symbols #############################################################
######################################################################################################################

def get_symbol_exchange(symbols_list):
    df_symbols = pd.read_csv(file_name_symbols, sep = '|')
    return df_symbols[df_symbols['Symbol'].isin(symbols_list)]['Exchange'].unique()
    
    
def get_exchange_list():
    exhange_list = ['NASDAQ', 'ASX', 'SGX', 'LSE', 'NYSE', 'FOREX', 'TSX', 'AMEX']
    yahoo_symbol = ['', '.AX', '.SI', '.L', '', '=X', '.TO', '']    
    return pd.DataFrame(list(zip(exhange_list, yahoo_symbol)), columns = ['Symbol', 'yahooo_abbr'])
    
    


def get_exchange_symbols(exchange, yahoo_abbr):
    ticker_symbol = []
    ticker_name = []    
    
    
    session = requests.Session()
    retry = Retry(connect=10, backoff_factor=0.1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)        
    
    for alphabet in string.ascii_uppercase:        
        resp = session.get('http://eoddata.com/stocklist/'+exchange+'/'+alphabet+'.html')
        soup = bs.BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table', {'class': 'quotes'})

        for row in table.findAll('tr')[1:]:
            symbol = row.findAll('td')[0].text + yahoo_abbr
            name = row.findAll('td')[1].text
            ticker_symbol.append(symbol)
            ticker_name.append(name)


    ticker_symbol = list(map(lambda s: s.strip(), ticker_symbol))
    ticker_name = list(map(lambda s: s.strip(), ticker_name))
    symbols_df = pd.DataFrame(list(zip(ticker_symbol, ticker_name)), columns =['Symbol', 'Name'])
    symbols_df['Exchange'] = exchange 
    return symbols_df



def get_all_symbols(fresh=False, exchange_list=None, save_df=True):
    
    df_all_exchange_symbols = pd.DataFrame()
    
    if fresh==False:
        if platform.system() == 'Windows':
            c_time = os.path.getctime(file_name_symbols)
            file_creation_dt = datetime.datetime.fromtimestamp(c_time).strftime('%Y-%m-%d')
            print(' File last modifed at:' + file_creation_dt)
    
        return pd.read_csv(file_name_symbols, sep='|')

    
    df_exhanges = get_exchange_list()
    
    if (exchange_list is not None):
        df_exhanges = df_exhanges[df_exhanges['Symbol'].isin(exchange_list)]
        
        
    for i in range(0,df_exhanges.shape[0]):
        print('pulling symbols for  ' + df_exhanges['Symbol'][i])
        df_exhange_symbols = get_exchange_symbols(df_exhanges['Symbol'][i], df_exhanges['yahooo_abbr'][i])
        df_all_exchange_symbols = df_all_exchange_symbols.append(df_exhange_symbols)

    if(save_df):
        df_all_exchange_symbols.to_csv(file_name_symbols, sep='|', index=False)
        
    return df_all_exchange_symbols





'''interval one of [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]'''

def get_individual_quote_history(symbol='SBIN.NS', START_DATE=None, END_DATE=None, data_interval='1d'):
    period_from = unix_time_millis(datetime.datetime.strptime(START_DATE, '%Y-%m-%d'))
    period_to = unix_time_millis(datetime.datetime.strptime(END_DATE, '%Y-%m-%d'))

    session = requests.Session()
    retry = Retry(connect=10, backoff_factor=0.3)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)    
    session.mount('https://', adapter)        
    
    try:    
        conn_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?\
                    &period1={period_from}&period2={period_to}&interval={data_interval}'.format(**locals())

        res = session.get(conn_url)
        if res.status_code!=200:
            return None
        
        data = res.json()
    except requests.ConnectionError as e:
        print("OOPS!! Connection Error.")
        pass

    body = data['chart']['result'][0]    

    if 'timestamp' not in body:
        return None
    
    dt = datetime.datetime
    dt = pd.Series(map(lambda x: arrow.get(x).to('Europe/Vienna').datetime.replace(tzinfo=None), body['timestamp']), \
                   name='Datetime')
    df = pd.DataFrame(body['indicators']['quote'][0], index=dt)
    dg = pd.DataFrame(body['timestamp'])
    df = df.rename(columns={"open": "Open", "close": "Close", "high": "High", "low": "Low", 'volume': 'Volume'})
    df_adj = pd.DataFrame(body['indicators']['adjclose'][0], index=dt)
    df['Adj_Close'] = df_adj['adjclose']
    df['Symbol'] = symbol
    return df.loc[:, ('Open', 'High', 'Low', 'Close', 'Volume', 'Adj_Close','Symbol')]


        

    













