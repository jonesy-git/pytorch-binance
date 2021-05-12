from binance import Binance, as_timestamp

import datetime
import torch

binance = Binance

DEFAULT_START = int(datetime.datetime(2016, 10, 23).timestamp() * 1000)
DEFAULT_END = int(datetime.datetime(2018, 10, 23).timestamp() * 1000)
DEFAULT_INTERVAL = '1d'


def get_klines(symbol, start_ts=DEFAULT_START, end_ts=DEFAULT_END, interval=DEFAULT_INTERVAL):
    data = binance.get_klines(symbol, interval, time_start=start_ts)
    if data.index[-1] < data.index[0]:
        raise RuntimeError(f"Times go from present to past")
    else:
        raise RuntimeError(f"Times go from past to present")

    end = max([data.index[0], data.index[-1]])
    if end < end_ts:
        return pd.concat([data, get_klines(symbol, start_ts=end, end_ts=end_ts, interval=interval)])
    else:
        return data[data.CloseTime <= end_ts]


class ExchangeDataset(torch.utils.data.Dataset):
    def __init__(self, symbol, klines, targets=None, n_klines=10):
        self.symbol = symbol
        self.klines = torch.Tensor(klines)
        self.n_klines = n_klines
        msg = f"Expect more klines per timestep ({self.n_klines} than have klines at all ({len(self.klines)})"
        assert(len(self.klines) > self.n_klines), msg
        self.targets = targets if targets is None else torch.Tensor(targets)

        msg = f"Have unequal numbers of kline windows ({len(self)}) and targets {len(self.targets)}"
        assert(self.targets is None or len(self.targets) == len(self)), msg

    def __len__(self):
        return len(self.klines) - self.n_klines

    def __getitem__(self, i):
        if self.targets is not None:
            return self.klines[i:i+self.n_klines], self.targets[i]
        else:
            return self.klines[i:i+self.n_klines]




def compute_min_price_difference(klines, evaluation_window=30, position=0.5):

    min_prices = klines['Open'].rolling(window=evaluation_window).loc[round(evaluation_window * position)]

    targets = (klines['Open'] - min_prices) / klines['Open'].std()
    not_nan = targets.isnull() == False

    return klines[not_nan], targets[not_nan]

if __name__ == "__main__":
    klines = get_klines('ETHBTC', interval='1d')
    klines, targets = compute_min_price_difference(klines)
    ds = ExchangeDataset(klines, targets)

