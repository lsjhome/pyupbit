import json
import logging
from urllib.parse import urlencode
from datetime import datetime, timedelta

import requests
import jwt


class PyUpbit():
    """
    Upbit API Wrapper
    https://docs.upbit.com/v1.0/reference
    """

    def __init__(self, access_key=None, secret_key=None):
        """
        Args:
            access_key(str): access key
            secret_key(str): secret key
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.markets = self._load_markets()

    #################
    # QUOTATION API #
    #################

    def get_market_all(self):
        """Available Market Code in Upbit
        https://docs.upbit.com/v1.0/reference#%EB%A7%88%EC%BC%93-%EC%BD%94%EB%93%9C-%EC%A1%B0%ED%9A%8C
        Returns:
            list(dictionary array): available market code and its name
        Examples:
            >>> upbit.get_minutes_candles()
            [{'market': 'KRW-BTC', 'korean_name': '비트코인', 'english_name': 'Bitcoin'},
             {'market': 'KRW-DASH', 'korean_name': '대시', 'english_name': 'Dash'},
             {'market': 'KRW-ETH', 'korean_name': '이더리움', 'english_name': 'Ethereum'},
             ...
             {'market': 'BTC-SERV', 'korean_name': '서브토큰', 'english_name': 'Serve'},
             {'market': 'BTC-ANKR', 'korean_name': '앵커', 'english_name': 'Ankr'}]
        """
        url = 'https://api.upbit.com/v1/market/all'
        return self._get(url)

    def get_minutes_candles(self, unit, market, to=None, count=None):
        """Minute Candle
        https://docs.upbit.com/v1.0/reference#%EB%B6%84minute-%EC%BA%94%EB%93%A4-1
        Args:
            unit(int):
            market(str): market code
            to(str): last candle time (exclusive) Format: "YYYY-MM-ddThh:mm:ss-ms:ns"
            count(int): the number of candles, max 200
        Returns:
            list(dictionary array): minutes candles
        Examples:
            >>> upbit.get_minutes_candels(unit=1, market='KRW-BTC', count=100, to='2019-03-08T03:40:00-00:00')
            [{'market': 'KRW-BTC', 'candle_date_time_utc': '2019-03-08T03:39:00',
              'candle_date_time_kst': '2019-03-08T12:39:00', 'opening_price': 4304000.0,
              'high_price': 4305000.0, 'low_price': 4303000.0, 'trade_price': 4303000.0,
              'timestamp': 1552016397275, 'candle_acc_trade_price': 20570004.54885,
              'candle_acc_trade_volume': 4.77935686, 'unit': 1},
             {'market': 'KRW-BTC', 'candle_date_time_utc': '2019-03-08T03:38:00',
              'candle_date_time_kst': '2019-03-08T12:38:00', 'opening_price': 4302000.0,
              'high_price': 4305000.0, 'low_price': 4300000.0, 'trade_price': 4305000.0,
              'timestamp': 1552016335894, 'candle_acc_trade_price': 22348540.81829,
              'candle_acc_trade_volume': 5.19359036,'unit': 1}, ... ]
        """
        url = 'https://api.upbit.com/v1/candles/minutes/%s' % str(unit)
        if unit not in [1, 3, 5, 10, 15, 30, 60, 240]:
            logging.error('invalid unit: %s' % str(unit))
            raise Exception('invalid unit: %s' % str(unit))
        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)

        params = {'market': market}
        if to is not None:
            params['to'] = to
        if count is not None:
            params['count'] = count
        return self._get(url, params=params)

    def get_days_candles(self, market, to=None, count=None):
        """Day Candle
        https://docs.upbit.com/v1.0/reference#%EC%9D%BCday-%EC%BA%94%EB%93%A4-1
        Args:
            market(str): market code
            to(str): last candle time (exclusive) Format: "YYYY-MM-ddThh:mm:ss-ms:ns"
            count(int): the number of candles, max 200
        Returns:
            list(dictionary array): daily candles
        Examples:
            >>> upbit.get_days_candles(market='KRW-BTC', count=200, to='2019-03-08T03:40:00-00:00')
            [{'market': 'KRW-BTC', 'candle_date_time_utc': '2019-03-08T00:00:00',
              'candle_date_time_kst': '2019-03-08T09:00:00', 'opening_price': 4289000.0,
              'high_price': 4358000.0, 'low_price': 4234000.0, 'trade_price': 4310000.0,
              'timestamp': 1552089599926, 'candle_acc_trade_price': 43201103063.97781,
              'candle_acc_trade_volume': 10012.80844409, 'prev_closing_price': 4289000.0,
              'change_price': 21000.0, 'change_rate': 0.0048962462},
             {'market': 'KRW-BTC', 'candle_date_time_utc': '2019-03-07T00:00:00',
              'candle_date_time_kst': '2019-03-07T09:00:00', 'opening_price': 4260000.0,
              'high_price': 4308000.0, 'low_price': 4258000.0, 'trade_price': 4289000.0,
              'timestamp': 1552003199477, 'candle_acc_trade_price': 21983491313.69089,
              'candle_acc_trade_volume': 5130.4641732, 'prev_closing_price': 4260000.0,
              'change_price': 29000.0, 'change_rate': 0.0068075117}, ... ]
        """
        url = 'https://api.upbit.com/v1/candles/days'
        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)

        params = {'market': market}
        if to is not None:
            params['to'] = to
        if count is not None:
            params['count'] = count
        return self._get(url, params=params)

    def get_weeks_candles(self, market, to=None, count=None):
        """Week Candle
        https://docs.upbit.com/v1.0/reference#%EC%A3%BCweek-%EC%BA%94%EB%93%A4-1
        Args:
            market(str): market code
            to(str): last candle time (exclusive) Format: "YYYY-MM-ddThh:mm:ss-ms:ns"
            count(int): the number of candles, max 76
        Returns:
            list(dictionary array): weekly candles
        Examples:
            >>> upbit.get_days_candles(market='KRW-BTC', count=200, to='2019-03-08T03:40:00-00:00')
            [{'market': 'KRW-BTC', 'candle_date_time_utc': '2019-03-08T00:00:00',
              'candle_date_time_kst': '2019-03-08T09:00:00', 'opening_price': 4289000.0,
              'high_price': 4358000.0, 'low_price': 4234000.0, 'trade_price': 4310000.0,
              'timestamp': 1552089599926, 'candle_acc_trade_price': 43201103063.97781,
              'candle_acc_trade_volume': 10012.80844409, 'prev_closing_price': 4289000.0,
              'change_price': 21000.0, 'change_rate': 0.0048962462},
             {'market': 'KRW-BTC', 'candle_date_time_utc': '2019-03-07T00:00:00',
              'candle_date_time_kst': '2019-03-07T09:00:00', 'opening_price': 4260000.0,
              'high_price': 4308000.0, 'low_price': 4258000.0, 'trade_price': 4289000.0,
              'timestamp': 1552003199477, 'candle_acc_trade_price': 21983491313.69089,
              'candle_acc_trade_volume': 5130.4641732, 'prev_closing_price': 4260000.0,
              'change_price': 29000.0, 'change_rate': 0.0068075117}, ... ]
        """
        url = 'https://api.upbit.com/v1/candles/weeks'
        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)
        params = {'market': market}
        if to is not None:
            params['to'] = to
        if count is not None:
            params['count'] = count
        return self._get(url, params=params)

    def get_months_candles(self, market, to=None, count=None):
        """Month Candle
        https://docs.upbit.com/v1.0/reference#%EC%9B%94month-%EC%BA%94%EB%93%A4-1
        Args:
            market(str): market code
            to(str): last candle time (exclusive) Format: "YYYY-MM-ddThh:mm:ss-ms:ns"
            count(int): the number of candles, max 19
        Returns:
            list(dictionary array): monthly candles
        Examples:
            >>> upbit.get_months_candles(market='KRW-BTC', count=200, to='2019-03-08T03:40:00-00:00')
            [{'market': 'KRW-BTC', 'candle_date_time_utc': '2019-03-01T00:00:00',
              'candle_date_time_kst': '2019-03-01T09:00:00', 'opening_price': 4240000.0,
              'high_price': 4396000.0, 'low_price': 4120000.0, 'trade_price': 4293000.0,
              'timestamp': 1552360767060, 'candle_acc_trade_price': 314743042514.0192,
              'candle_acc_trade_volume': 73542.30987779, 'first_day_of_period': '2019-03-01'},
             {'market': 'KRW-BTC', 'candle_date_time_utc': '2019-02-01T00:00:00',
              'candle_date_time_kst': '2019-02-01T09:00:00', 'opening_price': 3775000.0,
              'high_price': 4554000.0, 'low_price': 3735000.0, 'trade_price': 4240000.0,
              'timestamp': 1551398399461, 'candle_acc_trade_price': 613176329271.873,
              'candle_acc_trade_volume': 149447.01330811, 'first_day_of_period': '2019-02-01'},
              ... ]
        """
        url = 'https://api.upbit.com/v1/candles/months'
        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)
        params = {'market': market}
        if to is not None:
            params['to'] = to
        if count is not None:
            params['count'] = count
        return self._get(url, params=params)

    def get_trades_ticks(self, market, to=None, count=None, cursor=None):
        """Tick records
        https://docs.upbit.com/v1.0/reference#%EC%8B%9C%EC%84%B8-%EC%B2%B4%EA%B2%B0-%EC%A1%B0%ED%9A%8C
        Args:
            market(str): market
            to(str): the last transaction time. Format: "HHmmss" or "HH:mm:ss"
            count(int): the number of candles, max 500
            cursor(str): sequentialId, Can an be used for verifying trade uniqueness but does not gurantee the order of trade
        Returns:
            list(dictionary array): Tick records
        Examples:
            >>> upbit.get_trades_ticks(market='KRW-BTC', count=500)
            [{'market': 'KRW-BTC', 'trade_date_utc': '2019-03-12',
              'trade_time_utc': '06:36:59', 'timestamp': 1552372619000,
              'trade_price': 4300000.0, 'trade_volume': 0.01164652,
              'prev_closing_price': 4337000.0, 'change_price': -37000.0,
              'ask_bid': 'ASK', 'sequential_id': 1552372619000002},
             {'market': 'KRW-BTC', 'trade_date_utc': '2019-03-12',
              'trade_time_utc': '06:36:59', 'timestamp': 1552372619000,
              'trade_price': 4300000.0, 'trade_volume': 0.01164652,
              'prev_closing_price': 4337000.0, 'change_price': -37000.0,
              'ask_bid': 'ASK', 'sequential_id': 1552372619000001},
              ... ]
        """
        url = 'https://api.upbit.com/v1/trades/ticks'
        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)
        params = {'market': market}
        if to is not None:
            params['to'] = to
        if count is not None:
            params['count'] = count
        if cursor is not None:
            params['cursor'] = cursor
        return self._get(url, params=params)

    def get_ticker(self, markets):
        """
        Coin status
        https://docs.upbit.com/v1.0/reference#%EC%8B%9C%EC%84%B8-ticker-%EC%A1%B0%ED%9A%8C
        Args:
            markets(list): market list
        Returns:
            list(dictionary array): Coin status
        Examples:
            >>> upbit.get_ticker(markets=['KRW-BTC', 'KRW-ETH'])
            [{'market': 'KRW-BTC', 'trade_date': '20190312', 'trade_time': '072417',
              'trade_date_kst': '20190312', 'trade_time_kst': '162417',
              'trade_timestamp': 1552375457000, 'opening_price': 4336000.0,
              'high_price': 4337000.0, 'low_price': 4269000.0, 'trade_price': 4310000.0,
              'prev_closing_price': 4337000.0, 'change': 'FALL', 'change_price': 27000.0,
              'change_rate': 0.0062255015, 'signed_change_price': -27000.0,
              'signed_change_rate': -0.0062255015, 'trade_volume': 0.03176198,
              'acc_trade_price': 16360421608.77494, 'acc_trade_price_24h': 47680186655.6552,
              'acc_trade_volume': 3810.83151302, 'acc_trade_volume_24h': 11061.81267824,
              'highest_52_week_price': 10963000.0, 'highest_52_week_date': '2018-05-06',
              'lowest_52_week_price': 3562000.0, 'lowest_52_week_date': '2018-12-15',
              'timestamp': 1552375457797},
             {'market': 'KRW-ETH', 'trade_date': '20190312', 'trade_time': '072409',
              'trade_date_kst': '20190312', 'trade_time_kst': '162409',
              'trade_timestamp': 1552375449000, 'opening_price': 148500.0,
              'high_price': 149050.0, 'low_price': 143850.0, 'trade_price': 146500.0,
              'prev_closing_price': 148400.0, 'change': 'FALL', 'change_price': 1900.0,
              'change_rate': 0.0128032345, 'signed_change_price': -1900.0,
              'signed_change_rate': -0.0128032345, 'trade_volume': 0.33050539,
              'acc_trade_price': 5375953370.264639, 'acc_trade_price_24h': 12998227891.604963,
              'acc_trade_volume': 36749.28501606, 'acc_trade_volume_24h': 88215.44872648,
              'highest_52_week_price': 926200.0, 'highest_52_week_date': '2018-05-06',
              'lowest_52_week_price': 92450.0, 'lowest_52_week_date': '2018-12-15',
          'timestamp': 1552375449415}, ...]
        """
        url = 'https://api.upbit.com/v1/ticker'
        if not isinstance(markets, list):
            logging.error('invalid parameter: markets should be list')
            raise Exception('invalid parameter: markets should be list')

        if len(markets) == 0:
            logging.error('invalid parameter: no markets')
            raise Exception('invalid parameter: no markets')

        valid_market = []
        for market in markets:
            if market not in self.markets:
                logging.error('invalid market: %s' % market)
                raise Exception('invalid market: %s' % market)
            else:
                valid_market.append(market)

        markets_data = ','.join(valid_market)
        params = {'markets': markets_data}
        return self._get(url, params=params)

    def get_orderbook(self, markets):
        """
        Orderbook status
        https://docs.upbit.com/v1.0/reference#%ED%98%B8%EA%B0%80-%EC%A0%95%EB%B3%B4-%EC%A1%B0%ED%9A%8C
        Args:
            markets(list): market list
        Returns:
            list(dictionary array): orderbook status by market
        Examples:
            >>> upbit.get_orderbook(markets=['KRW-BTC', 'KRW-ETH'])
            [{'market': 'KRW-BTC',
              'orderbook_units': [{'ask_price': 4334000.0, 'ask_size': 0.83114626,
                                   'bid_price': 4332000.0, 'bid_size': 0.23555884},
                                  {'ask_price': 4335000.0, 'ask_size': 2.19284252,
                                   'bid_price': 4331000.0, 'bid_size': 0.28299738},
                                    ...
                                  {'ask_price': 4348000.0, 'ask_size': 0.2831,
                                   'bid_price': 4317000.0, 'bid_size': 0.4934},
                                  {'ask_price': 4349000.0, 'ask_size': 0.68444228,
                                   'bid_price': 4315000.0, 'bid_size': 0.61088748}],
                                   'timestamp': 1552378707204,
                                   'total_ask_size': 24.47313993,
                                   'total_bid_size': 14.60604858},
             {'market': 'KRW-ETH',
              'orderbook_units': [{'ask_price': 149000.0, 'ask_size': 48.13473522,
                                   'bid_price': 148900.0, 'bid_size': 49.69651787},
                                  {'ask_price': 149050.0, 'ask_size': 23.5,
                                   'bid_price': 148850.0, 'bid_size': 23.93356903},
                                   ...
                                  {'ask_price': 149700.0, 'ask_size': 0.141,
                                   'bid_price': 148250.0, 'bid_size': 0.00404721},
                                  {'ask_price': 149750.0, 'ask_size': 0.00400667,
                                   'bid_price': 148200.0, 'bid_size': 0.3}],
                                  'timestamp': 1552378707204,
                                  'total_ask_size': 613.01446061,
                                  'total_bid_size': 305.53846036}]

        """
        url = 'https://api.upbit.com/v1/orderbook'
        if not isinstance(markets, list):
            logging.error('invalid parameter: markets should be list')
            raise Exception('invalid parameter: markets should be list')

        if len(markets) == 0:
            logging.error('invalid parameter: no markets')
            raise Exception('invalid parameter: no markets')

        valid_market = []
        for market in markets:
            if market not in self.markets:
                logging.error('invalid market: %s' % market)
                raise Exception('invalid market: %s' % market)
            else:
                valid_market.append(market)

        markets_data = ','.join(valid_market)
        params = {'markets': markets_data}
        return self._get(url, params=params)

    ################
    # EXCHANGE API #
    ################

    def get_accounts(self):
        """show all accounts status
        https://docs.upbit.com/v1.0/reference#%EC%9E%90%EC%82%B0-%EC%A0%84%EC%B2%B4-%EC%A1%B0%ED%9A%8C
        Returns:
            list(dictionary array): Accunt status by currency
        Examples:
            >>> upbit.get_accounts()
            [{'currency': 'BTC', 'balance': '0.001', 'locked': '0.0',
              'avg_buy_price': '4082000', 'avg_buy_price_modified': False,
              'unit_currency': 'KRW', 'avg_krw_buy_price': '4082000', 'modified': False},
             {'currency': 'KRW', 'balance': '35001838.38973361', 'locked': '0.0',
              'avg_buy_price': '0', 'avg_buy_price_modified': True,
              'unit_currency': 'KRW', 'avg_krw_buy_price': '0', 'modified': True},
             {'currency': 'USDT', 'balance': '0.00006577', 'locked': '0.0',
              'avg_buy_price': '1146.36', 'avg_buy_price_modified': False,
              'unit_currency': 'KRW', 'avg_krw_buy_price': '1146.36', 'modified': False}]
        """
        url = 'https://api.upbit.com/v1/accounts'
        return self._get(url, headers=self._get_headers())

    def get_chance(self, market):
        """Available order by market
        https://docs.upbit.com/v1.0/reference#%EC%A3%BC%EB%AC%B8-%EA%B0%80%EB%8A%A5-%EC%A0%95%EB%B3%B4
        Args:
            market(str): market code
        Returns:
            dict: bid_fee, ask_fee, market, bid_account, ask_account
        Examples:
            >>> upbit.get_chance('KRW-BTC')
            {'bid_fee': '0.0005', 'ask_fee': '0.0005',
             'market': {'id': 'KRW-BTC', 'name': 'BTC/KRW',
                        'order_types': ['limit'], 'order_sides': ['ask', 'bid'],
                        'bid': {'currency': 'KRW', 'price_unit': None, 'min_total': 1000},
                        'ask': {'currency': 'BTC', 'price_unit': None, 'min_total': 1000},
                        'max_total': '1000000000.0', 'state': 'active'},
             'bid_account': {'currency': 'KRW', 'balance': '35001838.38973361',
                             'locked': '0.0', 'avg_buy_price': '0',
                             'avg_buy_price_modified': True, 'unit_currency': 'KRW',
                             'avg_krw_buy_price': '0', 'modified': True},
             'ask_account': {'currency': 'BTC', 'balance': '0.001', 'locked': '0.0',
                             'avg_buy_price': '4082000', 'avg_buy_price_modified': False,
                             'unit_currency': 'KRW', 'avg_krw_buy_price': '4082000',
                             'modified': False}}
        """
        url = 'https://api.upbit.com/v1/orders/chance'
        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)
        query = {'market': market}
        return self._get(url, headers=self._get_headers(query), params=query)

    def get_order(self, uuid, identifier=None):
        """check order status
        https://docs.upbit.com/v1.0.1/reference#%EA%B0%9C%EB%B3%84-%EC%A3%BC%EB%AC%B8-%EC%A1%B0%ED%9A%8C
        Args:
            uuid(str): order uuid
            identifier(str): inquery key set by user, default None
        Returns:
            dict: order status
        Examples
            >>> upbit.get_order(uuid='80bca2cc-fdbe-44a6-9c5d-328c65dea5b3')
            {'uuid': '80bca2cc-fdbe-44a6-9c5d-328c65dea5b3',
             'side': 'ask',
             'ord_type': 'limit',
             'price': '350.0',
             'state': 'done',
             'market': 'KRW-XRP',
             'created_at': '2019-03-14T15:11:00+09:00',
             'volume': '2.0',
             'remaining_volume': '0.0',
             'reserved_fee': '0.0',
             'remaining_fee': '0.0',
             'paid_fee': '0.35',
             'locked': '0.0',
             'executed_volume': '2.0',
             'trades_count': 1,
             'trades': [{'market': 'KRW-XRP',
               'uuid': '9587dbfe-8e74-4c7c-a7cd-878344942a7f',
               'price': '350.0',
               'volume': '2.0',
               'funds': '700.0',
               'side': 'ask'}]}
        """

        url = 'https://api.upbit.com/v1/order'
        if not any([uuid, identifier]):
            logging.error('either uuid or identifer are required')
            raise Exception('either uuid or identifer are required')
        try:
            query = {}
            query_pre = {'uuid': uuid, 'identifier': identifier}
            for key, value in query_pre.items():
                if value:
                    query[key] = value
            return self._get(url, headers=self._get_headers(query), params=query)
        except Exception as e:
            logging.error(e)
            raise Exception(e)

    def get_orders(self, market, state, page=1, order_by='asc'):
        """get order list
        Args:
            market(str): market code
            state(str): order status
            page(int): number of page, default 1
            order_by(str): asc(default), desc
        Returns:
            list(dictionary array): order list given by market and order state

        Examples:
            >>> upbit.upbit.get_orders('KRW-XRP', state='done')
            [{'uuid': '5364ac5b-324d-4ad9-9c66-ae63fe5c9880',
              'side': 'bid',
              'ord_type': 'limit',
              'price': '312.0',
              'state': 'done',
              'market': 'KRW-XRP',
              'created_at': '2018-09-18T12:31:11+09:00',
              'volume': '40.0',
              'remaining_volume': '0.0',
              'reserved_fee': '6.24',
              'remaining_fee': '0.0',
              'paid_fee': '6.24',
              'locked': '0.0',
              'executed_volume': '40.0',
              'trades_count': 1}, ...]
        """

        url = 'https://api.upbit.com/v1/orders'
        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)

        if state not in ['wait', 'done', 'cancel']:
            logging.error('invalid state: %s' % state)
            raise Exception('invalid state: %s' % state)

        if order_by not in ['asc', 'desc']:
            logging.error('invalid order_by: %s' % order_by)
            raise Exception('invalid order_by: %s' % order_by)

        query = {
            'market': market,
            'state': state,
            'page': page,
            'order_by': order_by
        }

        return self._get(url, headers=self._get_headers(query), params=query)

    def order(self, market, side, volume, price, ord_type='limit'):
        """send order to market
        https://docs.upbit.com/v1.0.1/reference#%EC%A3%BC%EB%AC%B8%ED%95%98%EA%B8%B0-1
        Args:
            market(str): market code
            side(str): bid or ask
            volume(int, float): order volume
            price(int, float): order price
            ord_type: order type, limit default
        Returns:
            dict
        Examples:
            >>> upbit.order('KRW-XRP', side='ask', volume=2, price=350)
            {'uuid': '80bca2cc-fdbe-44a6-9c5d-328c65dea5b3',
             'side': 'ask',
             'ord_type': 'limit',
             'price': '350.0',
             'state': 'wait',
             'market': 'KRW-XRP',
             'created_at': '2019-03-14T15:11:00+09:00',
             'volume': '2.0',
             'remaining_volume': '2.0',
             'reserved_fee': '0.0',
             'remaining_fee': '0.0',
             'paid_fee': '0.0',
             'locked': '2.0',
             'executed_volume': '0.0',
             'trades_count': 0}
        """
        url = 'https://api.upbit.com/v1/orders'

        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)

        if side not in ['bid', 'ask']:
            logging.error('invalid side: %s' % side)
            raise Exception('invalid side: %s' % side)

        if not self._is_valid_price(market, price):
            logging.error('invalid price: %s, %s' % (market, price))
            raise Exception('invalid price: %s, %s' % (market, price))

        if not self._is_above_min(market, price, volume):
            logging.error('less than minimum order amount: %s, %s, %s' % (market, price, volume))
            raise Exception('less than minimum order amount: %s, %s, %s' % (market, price, volume))

        data = {
            'market': market,
            'side': side,
            'volume': str(volume),
            'price': str(price),
            'ord_type': ord_type,
        }

        return self._post(url, headers=self._get_headers(data), data=data)

    def cancel_order(self, uuid):
        """cancel order
        https://docs.upbit.com/v1.0.1/reference#%EC%A3%BC%EB%AC%B8-%EC%B7%A8%EC%86%8C
        Args:
            uuid(str): order uuid
        Returns:
        Examples:
            >>> upbit.cancel_order()
        """
        url = 'https://api.upbit.com/v1/order'
        data = {'uuid': uuid}
        return self._delete(url, headers=self._get_headers(data), data=data)

    #################
    # HELPER METHOD #
    #################

    def _get(self, url, **kwargs):
        """requests get wrapper
        Args:
            url(str): url
            **kwargs: Arbitrary keyword arguments
        Returns:
            list(dictionary array)
        """
        resp = requests.get(url, **kwargs)
        if resp.status_code not in [200, 201]:
            logging.error('get(%s) failed(%d)' % (url, resp.status_code))
            if resp.text is not None:
                logging.error('resp: %s' % resp.text)
                raise Exception('request.get() failed(%s)' % resp.text)
            raise Exception(
                'request.get() failed(status_code:%d)' % resp.status_code)
        return json.loads(resp.text)

    def _post(self, url, **kwargs):
        """requests post wrapper
        Args:
            url(str): url
            **kwargs: Arbitrary keyword arguments
        Returns:
            list(dictionary array)
        """
        resp = requests.post(url, **kwargs)
        if resp.status_code not in [200, 201]:
            logging.error('post(%s) failed(%d)' % (url, resp.status_code))
            if resp.text is not None:
                raise Exception('request.post() failed(%s)' % resp.text)
            raise Exception(
                'request.post() failed(status_code:%d)' % resp.status_code)
        return json.loads(resp.text)

    def _delete(self, url, **kwargs):
        """requests delete wrapper
        Args:
            url(str): url
            **kwargs: Arbitrary keyword arguments
        Returns:
            list(dictionary array)
        """
        resp = requests.delete(url, **kwargs)
        if resp.status_code not in [200, 201]:
            logging.error('delete(%s) failed(%d)' % (url, resp.status_code))
            if resp.text is not None:
                raise Exception('request.delete() failed(%s)' % resp.text)
            raise Exception(
                'request.delete() failed(status_code:%d)' % resp.status_code)
        return json.loads(resp.text)

    def _load_markets(self):
        """load available market
        Returns:
            list: available market list
        Examples:
            >>> upbit._load_markets()
            ['KRW-BTC', 'KRW-DASH', 'KRW-ETH', 'BTC-ETH', 'BTC-LTC', ,,, ]
        """
        try:
            market_all = self.get_market_all()
            if market_all is None:
                return
            markets = [market['market'] for market in market_all]
            return markets
        except Exception as e:
            logging.error(e)
            raise Exception(e)

    def _get_token(self, query):
        """
        Args:
            query(dict): dictionary mapping for query string
        Returns:
            str: encoded json web token
        """
        payload = {
            'access_key': self.access_key,
            'nonce': int(datetime.timestamp(datetime.utcnow() + timedelta(hours=9))),
        }
        if query is not None:
            payload['query'] = urlencode(query)
        return jwt.encode(payload, self.secret_key, algorithm='HS256').decode('utf8')

    def _get_headers(self, query=None):
        """
        Args:
            query(dict): dictionary mapping for query string
        Returns:
            dict: headers for requests
        """
        headers = {'Authorization': 'Bearer %s' % self._get_token(query)}
        return headers

    def _is_valid_price(self, market, price):
        """check whether the price is valid or not
        https://upbit.com/service_center/guide
        Args:
            market(str): market code
            price(int, float): coin price
        Returns:
            bool
        """
        price = float(price)
        try:
            base = market.split('-')[0]
        except Exception as e:
            logging.error(e)
            raise Exception(e)

        if base not in ['KRW', 'BTC', 'ETH', 'USDT']:
            logging.error('invalid base coin %s' % base)
            raise Exception('invalid base coin %s' % base)

        if base == 'KRW':
            if price <= 10:
                if (price * 100) != int(price * 100):
                    return False
            elif price <= 100:
                if (price * 10) != int(price * 10):
                    return False
            elif price <= 1000:
                if price != int(price):
                    return False
            elif price <= 10000:
                if (price % 5) != 0:
                    return False
            elif price <= 100000:
                if (price % 10) != 0:
                    return False
            elif price <= 500000:
                if (price % 50) != 0:
                    return False
            elif price <= 1000000:
                if (price % 100) != 0:
                    return False
            elif price <= 2000000:
                if (price % 500) != 0:
                    return False
            elif (price % 1000) != 0:
                return False
            return True

        if base in ['BTC', 'ETH']:
            if (price * 100000000) != int(price * 100000000):
                print(price)
                return False
            return True

        if base == 'USDT':
            if (price * 1000) != int(price * 1000):
                return False
            return True

    def _is_above_min(self, market, price, volume):
        """check whether the order volume is above or equal to that of minimum requirement
        https://upbit.com/service_center/guide
        Args:
            market(str): market code
            price(int, float): coin price
            volume()
        Returns:
            bool
        """
        price = float(price)
        volume = float(volume)
        base = market.split('-')[0]

        if base == 'KRW':
            if price * volume < 500:
                return False
            return True

        if base in ['BTC', 'ETH', 'USDT']:
            if price * volume < 0.0005:
                return False
            return True
