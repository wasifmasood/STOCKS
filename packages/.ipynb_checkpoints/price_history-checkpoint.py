import sys

sys.path.insert(0, "D:/Users/masoodw/ML_FINANCE/")

from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd
import requests
import arrow
import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
#from pandas_datareader._utils import RemoteDataError


##########################################################################################
def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds())


##########################################################################################
'''interval one of [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]'''


def get_yahoo_symbol_snapshot(symbol='SBIN.NS', data_range='5m', data_interval='1d'):
    # print('pulling symbol:' + symbol)
    session = requests.Session()
    retry = Retry(connect=10, backoff_factor=0.3)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        conn_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={data_range}&interval={data_interval}'. \
            format(**locals())

        res = session.get(conn_url)
        if res.status_code != 200:
            return None

        data = res.json()
    except requests.ConnectionError as e:
        print("OOPS!! Connection Error.")
        pass

    if data['chart']['result'] is None:
        return None

    body = data['chart']['result'][0]

    if 'timestamp' not in body:
        return None

    dt = datetime.datetime
    dt = pd.Series(map(lambda x: arrow.get(x).to('Europe/Vienna').datetime.replace(tzinfo=None), body['timestamp']),
                   name='Datetime')
    df = pd.DataFrame(body['indicators']['quote'][0], index=dt)
    dg = pd.DataFrame(body['timestamp'])
    df = df.rename(columns={"open": "Open", "close": "Close", "high": "High", "low": "Low", 'volume': 'Volume'})
    df_adj = pd.DataFrame(body['indicators']['adjclose'][0], index=dt)
    df['Datetime'] = df.index
    df['Adj_Close'] = df_adj['adjclose']
    df['Symbol'] = symbol
    df = df.reset_index(drop=True)
    return df.loc[:, ('Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol', 'Adj_Close')]



##########################################################################################

def get_yahoo_symbol_history(symbol=None, start_date=None, end_date=None, data_interval='1d'):
    period_from = unix_time_millis(datetime.datetime.strptime(start_date, '%Y-%m-%d'))
    period_to = unix_time_millis(datetime.datetime.strptime(end_date, '%Y-%m-%d'))

    session = requests.Session()
    retry = Retry(connect=10, backoff_factor=0.3)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        conn_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?\
                    &period1={period_from}&period2={period_to}&interval={data_interval}'.format(**locals())

        res = session.get(conn_url)
        if res.status_code != 200:
            return None

        data = res.json()
    except requests.ConnectionError as e:
        print("OOPS!! Connection Error.")
        pass

    body = data['chart']['result'][0]

    if 'timestamp' not in body:
        return None

    dt = datetime.datetime
    dt = pd.Series(map(lambda x: arrow.get(x).to('Europe/Vienna').datetime.replace(tzinfo=None), body['timestamp']),
                   name='Datetime')
    df = pd.DataFrame(body['indicators']['quote'][0], index=dt)
    dg = pd.DataFrame(body['timestamp'])
    df = df.rename(columns={"open": "Open", "close": "Close", "high": "High", "low": "Low", 'volume': 'Volume'})
    df_adj = pd.DataFrame(body['indicators']['adjclose'][0], index=dt)
    df['Datetime'] = df.index
    df['Adj_Close'] = df_adj['adjclose']
    df['Symbol'] = symbol
    df=df.reset_index(drop=True)
    return df.loc[:, ('Datetime', 'Open', 'High', 'Low', 'Close', 'Volume',  'Symbol', 'Adj_Close')]


def get_price_history(symbol):
    df_history = pd.DataFrame()
    dt_start = '2015-01-01'
    dt_end = '2020-05-09'
    try:
        print('Pulling: ' + symbol + ' dates:'+dt_start + '-' + dt_end)
        df_history = get_yahoo_symbol_history(symbol, dt_start, dt_end, '1d')
        if df_history is None:
            return None
        df_history['Symbol'] = symbol
    except e:
        print('Error:' + e)
        pass
    return df_history


def extract_indices_history(symbol_list):
    print(symbol_list)
    # Run with Multi threading
    pool = ThreadPool(500)
    # Open the urls in their own threads and return the results
    results = pd.concat(pool.map(get_price_history, symbol_list))
    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()
    print('Symbols' + str(results['Symbol'].nunique()))
    print('Coverage %' + str((results['Symbol'].nunique() / results.shape[0]) * 100))
    print('Total records:' + str(results.shape))
    return results

def extract_index_history(symbol, dt_start, dt_end):
    df_history = pd.DataFrame()
    try:
        print('Pulling: ' + symbol + ' dates:'+dt_start + '-' + dt_end)
        df_history = get_yahoo_symbol_history(symbol, dt_start, dt_end, '1d')
        if df_history is None:
            return None
        df_history['Symbol'] = symbol
    except RemoteDataError:
        print('Symbol Not Found:' + symbol)
        pass
    return df_history

