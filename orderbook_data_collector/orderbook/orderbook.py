"""

@author: Zhishe

"""

import datetime as dt

from .redisOrderTree import OrderTree

class OrderException(Exception): pass
class OrderQuantityError(OrderException): pass
class OrderPriceError(OrderException): pass


class Order:
    def __init__(self, orderId, qty, price, timestamp):
        self.orderId = orderId
        self.qty = float(qty)  # not sure whether crypto trading allows fractional size
        self.price = float(price)
        self.timestamp = timestamp

    def processPriceLevel(self, book, tree, orderList, qtyToTrade):
        """
        Takes a list of orders at a certain price level as well as an incoming order
        and matches appropriate trades given the order quantity.
        """
        trades = []
        for order in orderList:
            if qtyToTrade <= 0:
                break
            if qtyToTrade < order.qty:
                tradeQty = qtyToTrade
                newBookQty = order.qty - qtyToTrade
                tree.updateOrderQuantity(order.orderId, newBookQty)
                qtyToTrade = 0
            elif qtyToTrade == order.qty:
                tradeQty = qtyToTrade
                tree.removeOrderById(order.orderId)  # hit bid or lift ask
                qtyToTrade = 0
            else:
                tradeQty = order.qty
                tree.removeOrderById(order.orderId)  # hit bid or lift ask
                qtyToTrade -= tradeQty

            transactionRecord = {'timestamp': book.getTimestamp(), 'price': order.price, 'qty': tradeQty}
            if tree.side == 'bid':
                transactionRecord['party1'] = ['bid', order.orderId]
                transactionRecord['party2'] = ['ask', None]
            else:
                transactionRecord['party1'] = ['ask', order.orderId]
                transactionRecord['party2'] = ['bid', None]
            trades.append(transactionRecord)
        return qtyToTrade, trades


class Bid(Order):
    def __init__(self, orderId, qty, price, timestamp):
        Order.__init__(self, orderId, qty, price, timestamp)
        self.side = 'bid'

    def limitOrder(self, book, bids, asks):
        trades = []
        orderInBook = None
        qtyToTrade = self.qty
        # __len__ of asks is called as __bool__ is not implemented
        while (asks and self.price >= asks.minPrice() and qtyToTrade > 0):
            bestPriceAsks = [Ask(x['orderId'], x['qty'], x['price'], x['timestamp']) for x in asks.minPriceList()]
            qtyToTrade, newTrades = self.processPriceLevel(book, asks, bestPriceAsks, qtyToTrade)
            trades += newTrades
        # if volume remains, add to book
        if qtyToTrade > 0:
            self.qty = qtyToTrade
            bids.insertOrder(self)
            orderInBook = self
        return trades, orderInBook

    def marketOrder(self, book, bids, asks):
        trades = []
        qtyToTrade = self.qty
        # __len__ of asks is called as __bool__ is not implemented
        while qtyToTrade > 0 and asks:
            bestPriceAsks = [Ask(x['orderId'], x['qty'], x['price'], x['timestamp']) for x in asks.minPriceList()]
            qtyToTrade, newTrades = self.processPriceLevel(book, asks, bestPriceAsks, qtyToTrade)
            trades += newTrades
        return trades


class Ask(Order):
    def __init__(self, orderId, qty, price, timestamp):
        Order.__init__(self, orderId, qty, price, timestamp)
        self.side = 'ask'

    def limitOrder(self, book, bids, asks):
        trades = []
        orderInBook = None
        qtyToTrade = self.qty
        # __len__ of bids is called as __bool__ is not implemented
        while (bids and self.price <= bids.maxPrice() and qtyToTrade > 0):
            bestPriceBids = [Bid(x['orderId'], x['qty'], x['price'], x['timestamp']) for x in bids.maxPriceList()]
            qtyToTrade, newTrades = self.processPriceLevel(book, bids, bestPriceBids, qtyToTrade)
            trades += newTrades
        # if volume remains, add to book
        if qtyToTrade > 0:
            self.qty = qtyToTrade
            asks.insertOrder(self)
            orderInBook = self
        return trades, orderInBook

    def marketOrder(self, book, bids, asks):
        trades = []
        qtyToTrade = self.qty
        # __len__ of bids is called as __bool__ is not implemented
        while qtyToTrade > 0 and bids:
            bestPriceBids = [Bid(x['orderId'], x['qty'], x['price'], x['timestamp']) for x in bids.maxPriceList()]
            qtyToTrade, newTrades = self.processPriceLevel(book, bids, bestPriceBids, qtyToTrade)
            trades += newTrades
        return trades


class OrderBook:
    def __init__(self, exchange, symbol, red):
        self.exch = exchange
        self.symbol = symbol
        self.red = red

        self.bids = OrderTree(exchange, symbol, 'bid', red)
        self.asks = OrderTree(exchange, symbol, 'ask', red)

        self._lastTimestamp = None

    def processOrder(self, order):
        orderInBook = None

        if order.qty <= 0:
            raise OrderQuantityError('order.qty must be > 0')
        if order.price <= 0:
            raise OrderPriceError('order.price must be > 0')

        trades, orderInBook = order.limitOrder(self, self.bids, self.asks)

        return trades, orderInBook

    def cancelOrder(self, side, orderId):
        """delete
        """
        if side == 'bid':
            self.bids.removeOrderById(orderId)
        else:
            self.asks.removeOrderById(orderId)

    def getBestBid(self):
        return self.bids.maxPrice()
    
    def getWorstBid(self):
        return self.bids.minPrice()
    
    def getBestAsk(self):
        return self.asks.minPrice()
    
    def getWorstAsk(self):
        return self.asks.maxPrice()

    def getTimestamp(self):
        t = dt.datetime.utcnow().timestamp()
        while t == self._lastTimestamp:
            t = dt.datetime.utcnow().timestamp()
        self._lastTimestamp = t
        return t

