import requests
from functools import cache



class Binance(object):
    _valid_intervals = ['S', 'm', 'H', 'd']
    _kline_column_names = ['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'QuoteAssetVolume',
                           'NumberOfTrades', 'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume', 'Ignore']

    def __init__(self, cachedir='./cache'):
        self.url_base = 'https://www.binance.com/api/v3/'
        self.endpoints = {
            'klines': 'klines',
            'exchangeInfo': 'exchangeInfo',
        }
        self.cache_dir = cachedir
        os.makedirs(self.cache_dir, exist_ok=True)
        self.symbols_path = os.path.join(self.cache_dir, 'symbols.csv')
        self.symbols = self.get_symbols()

    @staticmethod
    def make_query_string(**kwargs):
        if kwargs:
            return '?' + '&'.join([f'{key}={value}' for key, value in kwargs.items() if value is not None])
        else:
            return ''

    def get_klines(self, symbol, interval, time_start=None, time_end=None, limit=None):
        url = self.url_base + self.endpoints['klines']
        url += self.make_query_string(
            symbol=symbol,
            interval=interval,
            startTime=time_start,
            endTime=time_end,
            limit=limit
        )
        data = self.get(url, f"Couldn't get klines for {symbol} (url={url!r}): ")
        return pd.DataFrame(data, columns=self._kline_column_names).drop('Ignore', 1)

    def get_symbols(self):
        if os.path.isfile(self.symbols_path):
            return pd.read_csv(self.symbols_path)

        url = self.url_base + self.endpoints['exchangeInfo']

        exchange_info = self.get(url, f"Couldn't get exchange info (url={url!r}): ")
        symbol_dict = lambda s: {'name': s['symbol'], 'base': s['baseAsset'], 'quote': s['quoteAsset']}
        symbols = pd.DataFrame.from_records([symbol_dict(s) for s in exchange_info['symbols']])
        symbols.to_csv(self.symbols_path, index=False)
        return symbols

    def coins_to_symbol(self, A, B):
        coins = (A, B)
        match = (self.symbols.base == A) & (self.symbols.quote == B)
        match |= (self.symbols.base == B) & (self.symbols.quote == A)
        if match.sum() != 1:
            raise SymbolNotFoundError(f"Symbol found {match.sum().item()} matches for coins {coins}")

        return self.symbols[match]

    def symbol_to_coins(self, symbol):
        return self.symbols.set_index('name').loc[symbol]

    def assert_coin_exists(self, coin):
        if coin not in self.symbols.base.values and coin not in self.symbols.quote.values:
            raise CoinNotFoundError(f"Couldn't find coin: {coin!r}")



    @classmethod
    def check_times(cls, time_start, time_end):

        time_start, time_end = as_timestamp(time_start), as_timestamp(time_end)

        if not time_end > time_start:
            time_start, time_end = as_datetime(time_start), as_datetime(time_end)
            raise ValueError(f"{time_end = } is not greater than {time_start = }")

        return time_start, time_end


    def get(self, url, msg=''):
        response = requests.get(url)
        if response.status_code != 200:
            raise requests.RequestException(msg + response.text)
        return response.json()

    @cache
    def get_exchange(self, symbol, time_interval, **kwargs):
        return Exchange(self.symbol_to_coins(symbol), time_interval, binance=self, **kwargs)


    def get_fiat_exchanges(self, exchange, fiat='EUR', **kwargs):
        exchange_kws = exchange.kwargs.copy()
        exchange_kws.update(kwargs)

        return [self.get_exchange((coin, fiat), **exchange_kws) for coin in exchange.coins]

class Exchange(object):
    def __init__(self, coins, time_interval='10m', time_start=None, time_end=None, binance=None, fiat='EUR'):
        self.binance = binance or Binance()
        self.coins = coins
        self.symbol = self.binance.coins_to_symbol(*coins)
        self.coins = self.base, self.quote = self.binance.symbol_to_coins(self.symbol)
        self.time_interval, self.time_start, self.time_end = time_interval, time_start, time_end
        self.fiat = fiat

        self.data = self.collect_data()

    def buy_batch(self, amounts, timestamp):
        data = self.data[self.data.index >= timestamp]
        return [self.buy(a, timestamp, data) for a in amounts]

    def buy(self, amount, timestamp, data=None):
        if data is None:
            data = self.data[self.data.index >= timestamp]
        payed = 0
        for i, row in data.iterrows():
            if amount > 0:
                buy = min(amount, row.Volume)
                payed += buy * row.AvgPrice
                amount -= buy
            else:
                break

        if amount:
            raise OrderNotFilled(f'No more {self.base} to buy in {self.symbol}')

        return payed

    def sell(self, amount, timestamp):
        data = self.data[self.data.index >= timestamp]
        bought = 0
        for i, row in data.iterrows():
            if amount > 0:
                sell = min(amount, row.QuoteAssetVolume)
                bought += sell * row.AvgPrice
                amount -= sell
            else:
                break

        if amount:
            raise OrderNotFilled(f'No more {self.base} to buy in {self.symbol}')

        return bought

    def collect_data(self, time_interval, time_start, time_end):


        data = self.binance.get_klines(self.symbol.name.item(),
                                            interval=time_interval,
                                            time_start=time_start,
                                            time_end=time_end)

        fiat_base, fiat_quote = self.binance.get_fiat_exchanges(self, self.fiat)
        data[self.base+self.fiat] = fiat_base.data[['Open', 'Close']].mean(-1)
        data[self.quote+self.fiat] = fiat_quote.data[['Open', 'Close']].mean(-1)
        data['AvgPrice'] = data[['Open', 'Close']].mean(-1)
        return data.set_index('OpenTime')



    @property
    def kwargs(self):
        return {'time_start': self.time_start,
                'time_end': self.time_end,
                'time_interval': self.time_interval,
                'binance':self.binance,
                'fiats': self.fiats,
                'base_currency': self.base_currency}

    def __repr__(self):
        return f"{type(self).__name__}({self.symbol.base.item()!r}, {self.symbol.quote.item()!r}, " \
               f"time_start={self.time_start}, time_end={self.time_end}, binance={self.binance})"



# Errors

class SymbolNotFoundError(RuntimeError):
    pass

class CoinNotFoundError(RuntimeError):
    pass

class OrderNotFilled(RuntimeError):
    pass

def as_timestamp(dt):
    if isinstance(dt, (int, float)):
        return dt
    return dt.timestamp() * 1000

def as_datetime(ts):
    if isinstance(ts, datetime.datetime):
        return ts
    return datetime.datetime.fromtimestamp(ts / 1000)
