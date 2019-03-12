from pyupbit import PyUpbit
import unittest
import logging


class PyUpbitTest(unittest.TestCase):

    def test_get_market_all(self):
        upbit = PyUpbit()
        ret = upbit.get_market_all()
        self.assertIsNotNone(ret)
        self.assertNotEqual(len(ret), 0)
        logging.info(ret)

    def test_get_minutes_candles(self):
        upbit = PyUpbit()
        ret = upbit.get_minutes_candles(unit=1, market='KRW-BTC', count=100, to='2019-03-08T03:40:00-00:00')
        self.assertIsNotNone(ret)
        self.assertNotEqual(len(ret), 0)
        logging.info(ret)

    def test_get_days_candles(self):
        upbit = PyUpbit()
        ret = upbit.get_days_candles(market='KRW-ETH', count=200, to='2019-03-08T03:40:00-00:00')
        self.assertIsNotNone(ret)
        logging.info(ret)

    def test_get_weeks_candles(self):
        upbit = PyUpbit()
        ret = upbit.get_days_candles(market='KRW-BTC', count=76, to='2019-03-08T03:40:00-00:00')
        self.assertIsNotNone(ret)
        self.assertNotEqual(len(ret), 0)
        logging.info(ret)

    def test_get_months_candles(self):
        upbit = PyUpbit()
        ret = upbit.get_months_candles(market='KRW-BTC', count=200, to='2019-03-08T03:40:00-00:00')
        self.assertIsNotNone(ret)
        self.assertNotEqual(len(ret), 0)
        logging.info(ret)

    def test_get_ticker(self):
        upbit = PyUpbit()
        ret = upbit.get_ticker(['KRW-BTC', 'KRW-ETH'])
        self.assertIsNotNone(ret)
        self.assertNotEqual(len(ret), 0)
        logging.info(ret)

    def test_get_orderbook(self):
        upbit = PyUpbit()
        ret = upbit.get_orderbook(['KRW-BTC', 'KRW-ETH'])
        self.assertIsNotNone(ret)
        self.assertNotEqual(len(ret), 0)
        logging.info(ret)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
