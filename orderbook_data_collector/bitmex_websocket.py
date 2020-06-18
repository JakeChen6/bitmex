import websocket
import threading
import traceback
import time
import datetime
import json
import logging
import urllib
import math
from queue import Queue
from util.api_key import generate_nonce, generate_signature

from orderbook.orderbook import Bid, Ask, OrderBook
from db_writer import write_to_db

from bitmex_config import ACCOUNT

EXCH = 'BitMEX'

# This is what I'm asked to write into Redis
TARGET = ['orderBookL2', 'margin', 'position']


def helper_dict_clean(items):
    """
    Redis only accepts bytes, str, int, or float types.
    This helper converts None to empty string ''.
    """
    return {k: v if v is not None else '' for k, v in items}


# Naive implementation of connecting to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without polling the hell out of the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll really often if it wants.
class BitMEXWebsocket:

    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    def __init__(self, endpoint, symbol, red, api_key=None, api_secret=None):
        '''Connect to the websocket and initialize data stores.'''
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing WebSocket.")

        self.endpoint = endpoint
        self.symbol = symbol
        self.red = red

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret

        self.data = {}
        self.keys = {}
        self.exited = False

        # Redis
        # instantiate an orderbook in Redis
        self.orderbook = OrderBook(EXCH, symbol, red)

        # the key in Redis for margin info
        self.KEY_TEMPLATE_MARGIN = '%s-%s-margin-%s' % (EXCH, symbol, ACCOUNT)
        # the key in Redis for position info
        self.KEY_TEMPLATE_POSITION = '%s-%s-position-%s' % (EXCH, symbol, ACCOUNT)

        # MongoDB
        # the collection names to use in MongoDB
        collection_names = {'margin': self.KEY_TEMPLATE_MARGIN,
                            'position': self.KEY_TEMPLATE_POSITION,
                            'orderBookL2': '%s-%s-orderBookL2' % (EXCH, symbol)}

        # start the database writing thread
        self.db_queue = Queue()
        self.dbt = threading.Thread(target=write_to_db, args=(collection_names, self.db_queue))
        self.dbt.start()

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        wsURL = self.__get_url()
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL, symbol)
        self.logger.info('Connected to WS.')

        # Connected. Wait for partials
        self.__wait_for_symbol(symbol)
        if api_key:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')

    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self.ws.close()

    def get_instrument(self):
        '''Get the raw instrument data for this symbol.'''
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        instrument = self.data['instrument'][0]
        instrument['tickLog'] = int(math.fabs(math.log10(instrument['tickSize'])))
        return instrument

    def get_ticker(self):
        '''Return a ticker object. Generated from quote and trade.'''
        lastQuote = self.data['quote'][-1]
        lastTrade = self.data['trade'][-1]
        ticker = {
            "last": lastTrade['price'],
            "buy": lastQuote['bidPrice'],
            "sell": lastQuote['askPrice'],
            "mid": (float(lastQuote['bidPrice'] or 0) + float(lastQuote['askPrice'] or 0)) / 2
        }

        # The instrument has a tickSize. Use it to round values.
        instrument = self.data['instrument'][0]
        return {k: round(float(v or 0), instrument['tickLog']) for k, v in ticker.items()}

    def funds(self):
        '''Get your margin details.'''
        return self.red.hgetall(self.KEY_TEMPLATE_MARGIN)

    def positions(self):
        '''Get your positions.'''
        return self.red.hgetall(self.KEY_TEMPLATE_POSITION)

    def market_depth(self):
        '''Get market depth (orderbook). Returns all levels.'''
        return self.data['orderBookL2']

    def open_orders(self, clOrdIDPrefix):
        '''Get all your open orders.'''
        orders = self.data['order']
        # Filter to only open orders and those that we actually placed
        return [o for o in orders if str(o['clOrdID']).startswith(clOrdIDPrefix) and order_leaves_quantity(o)]

    def recent_trades(self):
        '''Get recent trades.'''
        return self.data['trade']

    #
    # End Public Methods
    #

    def __connect(self, wsURL, symbol):
        '''Connect to the websocket in a thread.'''
        self.logger.debug("Starting thread")

        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException('Couldn\'t connect to WS! Exiting.')

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if self.api_key:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            expires = generate_nonce()
            return [
                "api-expires: " + str(expires),
                "api-signature: " + generate_signature(self.api_secret, 'GET', '/realtime', expires, ''),
                "api-key:" + self.api_key
            ]
        else:
            self.logger.info("Not authenticating.")
            return []

    def __get_url(self):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
        symbolSubs = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
        genericSubs = ["margin"]

        subscriptions = [sub + ':' + self.symbol for sub in symbolSubs]
        subscriptions += genericSubs

        urlParts = list(urllib.parse.urlparse(self.endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))
        return urllib.parse.urlunparse(urlParts)

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not {'margin', 'position', 'order', 'orderBookL2'} <= set(self.data):
            time.sleep(0.1)

    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
            time.sleep(0.1)

    def __send_command(self, command, args=None):
        '''Send a raw command.'''
        if args is None:
            args = []
        self.ws.send(json.dumps({"op": command, "args": args}))

    def __on_message(self, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message, object_pairs_hook=helper_dict_clean)  # convert None to empty string ''
        self.logger.debug(json.dumps(message))

        table = message.get("table")
        action = message.get("action")
        try:
            if 'subscribe' in message:
                self.logger.debug("Subscribed to %s." % message['subscribe'])
            elif action:

                if table not in self.data:
                    self.data[table] = []

                # There are four possible actions from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row

                # we will give special attention to
                # 1. orderbook data
                # 2. balance data
                # 3. position data

                # orderbook, balance, and position data are written into Redis,
                # and when we query for them using this object's methods, the
                # object reads the data from Redis and return them to us.

                if action == 'partial':
                    self.logger.debug("%s: partial" % table)
                    self.data[table] = message['data']
                    # Keys are communicated on partials to let you know how to uniquely identify
                    # an item. We use it for updates.
                    self.keys[table] = message['keys']

                    # write snapshots into Redis
                    if table in TARGET and message['data']:  # non-empty message['data']
                        if table == 'margin':
                            # write margin info into Redis using hash
                            self.red.hset(self.KEY_TEMPLATE_MARGIN, mapping=message['data'][0])
                        elif table == 'position':
                            # write position info into Redis using hash
                            self.red.hset(self.KEY_TEMPLATE_POSITION, mapping=message['data'][0])
                        elif table == 'orderBookL2':
                            # I think timestamp should be included in message,
                            # however it's not, so here I manually create one.
                            timestamp = self.orderbook.getTimestamp()
                            buy_orders = []
                            sell_orders = []
                            for order in message['data']:
                                if order['side'] == 'Buy':
                                    buy_orders.append(order)
                                else:
                                    sell_orders.append(order)
                            buy_orders = list(map(lambda x: Bid(x['id'], x['size'], x['price'], timestamp), buy_orders))
                            sell_orders = list(map(lambda x: Ask(x['id'], x['size'], x['price'], timestamp), sell_orders))
                            self.orderbook.bids.insertManyOrders(buy_orders)
                            self.orderbook.asks.insertManyOrders(sell_orders)

                        # write into MongoDB
                        timestamp = datetime.datetime.utcnow().timestamp()
                        self.db_queue.put((timestamp, message))

                elif action == 'insert':
                    self.logger.debug('%s: inserting %s' % (table, message['data']))
                    self.data[table] += message['data']

                    # Limit the max length of the table to avoid excessive memory usage.
                    # Don't trim orders because we'll lose valuable state if we do.
                    if table not in ['order', 'orderBookL2'] and len(self.data[table]) > BitMEXWebsocket.MAX_TABLE_LEN:
                        self.data[table] = self.data[table][BitMEXWebsocket.MAX_TABLE_LEN // 2:]

                    # insert new orders into the orderbook in Redis
                    if table == 'orderBookL2' and message['data']:
                        timestamp = self.orderbook.getTimestamp()
                        buy_orders = []
                        sell_orders = []
                        for order in message['data']:
                            if order['size'] == 'Buy':
                                buy_orders.append(order)
                            else:
                                sell_orders.append(order)
                        buy_orders = list(map(lambda x: Bid(x['id'], x['size'], x['price'], timestamp), buy_orders))
                        sell_orders = list(map(lambda x: Ask(x['id'], x['size'], x['price'], timestamp), sell_orders))
                        self.orderbook.bids.insertManyOrders(buy_orders)
                        self.orderbook.asks.insertManyOrders(sell_orders)

                        # write into MongoDB
                        timestamp = datetime.datetime.utcnow().timestamp()
                        self.db_queue.put((timestamp, message))

                elif action == 'update':
                    self.logger.debug('%s: updating %s' % (table, message['data']))
                    # Locate the item in the collection and update it.
                    for updateData in message['data']:
                        item = find_by_keys(self.keys[table], self.data[table], updateData)
                        if not item:
                            return  # No item found to update. Could happen before push
                        item.update(updateData)
                        # Remove cancelled / filled orders
                        if table == 'order' and not order_leaves_quantity(item):
                            self.data[table].remove(item)

                    # update the snapshots in Redis
                    if table in TARGET and message['data']:
                        if table == 'margin':
                            self.red.hset(self.KEY_TEMPLATE_MARGIN, mapping=message['data'][0])
                        elif table == 'position':
                            self.red.hset(self.KEY_TEMPLATE_POSITION, mapping=message['data'][0])
                        elif table == 'orderBookL2':
                            bid_updates = []
                            ask_updates = []
                            for elm in message['data']:
                                if elm['size'] == 'Buy':
                                    if self.orderbook.bids.orderExists(elm['id']):
                                        bid_updates.append(elm)
                                else:
                                    if self.orderbook.asks.orderExists(elm['id']):
                                        ask_updates.append(elm)
                            bid_updates = [{'orderId': u['id'], 'mapping': {'qty': u['size']}} for u in bid_updates]
                            ask_updates = [{'orderId': u['id'], 'mapping': {'qty': u['size']}} for u in ask_updates]
                            self.orderbook.bids.updateManyOrders(bid_updates)
                            self.orderbook.asks.updateManyOrders(ask_updates)

                        # write into MongoDB
                        timestamp = datetime.datetime.utcnow().timestamp()
                        self.db_queue.put((timestamp, message))

                elif action == 'delete':
                    self.logger.debug('%s: deleting %s' % (table, message['data']))
                    # Locate the item in the collection and remove it.
                    for deleteData in message['data']:
                        item = find_by_keys(self.keys[table], self.data[table], deleteData)
                        self.data[table].remove(item)

                    # update the snapshots in Redis
                    if table == 'orderBookL2' and message['data']:
                        for elm in message['data']:
                            orderId = elm['id']
                            side = elm['side']
                            tree = self.orderbook.bids if side == 'Buy' else self.orderbook.asks
                            tree.removeOrderById(orderId)

                        # write into MongoDB
                        timestamp = datetime.datetime.utcnow().timestamp()
                        self.db_queue.put((timestamp, message))

                else:
                    raise Exception("Unknown action: %s" % action)
        except:
            self.logger.error(traceback.format_exc())

    def __on_error(self, error):
        '''Called on fatal websocket errors. We exit on these.'''
        if not self.exited:
            self.logger.error("Error : %s" % error)
            raise websocket.WebSocketException(error)

    def __on_open(self):
        '''Called when the WS opens.'''
        self.logger.debug("Websocket Opened.")

    def __on_close(self):
        '''Called on websocket close.'''
        self.logger.info('Websocket Closed')


# Utility method for finding an item in the store.
# When an update comes through on the websocket, we need to figure out which item in the array it is
# in order to match that item.
#
# Helpfully, on a data push (or on an HTTP hit to /api/v1/schema), we have a "keys" array. These are the
# fields we can use to uniquely identify an item. Sometimes there is more than one, so we iterate through all
# provided keys.
def find_by_keys(keys, table, matchData):
    for item in table:
        if all(item[k] == matchData[k] for k in keys):
            return item

def order_leaves_quantity(o):
    if o['leavesQty'] is None:
        return True
    return o['leavesQty'] > 0
