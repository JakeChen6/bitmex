"""

@author: Zhishe

"""


class OrderTree:
    def __init__(self, exchange, symbol, side, red):
        """
        exchange : str
        symbol: str
        side : str
        red : redis.Redis
        """
        self.exch = exchange
        self.symbol = symbol
        self.side = side
        self.red = red

        self.KEY_PRICE_TREE = '%s-%s-prices-%s' % (exchange, symbol, side)
        self.KEY_TEMPLATE_ORDER = '%s-%s-order-%%s' % (exchange, symbol)  # order id
        self.KEY_TEMPLATE_ORDERS_BY_PRICE = '%s-%s-%s-%%s' % (exchange, symbol, side)  # price

    def __len__(self):
        return self.red.zcard(self.KEY_PRICE_TREE)

    def getOrdersAtPrice(self, price):
        return self.red.lrange(self.KEY_TEMPLATE_ORDERS_BY_PRICE % price, 0, -1)

    def orderExists(self, orderId):
        return self.red.exists(self.KEY_TEMPLATE_ORDER % orderId)

    def insertOrder(self, order):
        """
        order : Order
        """
        price = order.price
        if not self.red.exists(self.KEY_TEMPLATE_ORDERS_BY_PRICE % price):
            self.red.zadd(self.KEY_PRICE_TREE, {price: price})

        self.red.hset(self.KEY_TEMPLATE_ORDER % order.orderId, mapping=order.__dict__)
        self.red.rpush(self.KEY_TEMPLATE_ORDERS_BY_PRICE % price, order.orderId)

    def updateOrder(self, orderId, mapping):
        self.red.hset(self.KEY_TEMPLATE_ORDER % orderId, mapping=mapping)

    def removeOrderById(self, orderId):
        order = self.red.hgetall(self.KEY_TEMPLATE_ORDER % orderId)
        self.red.lrem(self.KEY_TEMPLATE_ORDERS_BY_PRICE % order['price'], 0, orderId)
        if not self.red.exists(self.KEY_TEMPLATE_ORDERS_BY_PRICE % order['price']):
            self.red.zrem(self.KEY_PRICE_TREE, order['price'])
        self.red.delete(self.KEY_TEMPLATE_ORDER % orderId)

    def maxPrice(self):
        r = self.red.zrevrange(self.KEY_PRICE_TREE, 0, 0)
        if r:
            return float(r[0])
        else:
            return 0
    
    def minPrice(self):
        r = self.red.zrange(self.KEY_PRICE_TREE, 0, 0)
        if r:
            return float(r[0])
        else:
            return 0

    def maxPriceList(self):
        # in redis-py, pipeline is a transactional pipeline class by default
        with self.red.pipeline() as pipe:
            for orderID in self.red.lrange(self.KEY_TEMPLATE_ORDERS_BY_PRICE % self.maxPrice(), 0, -1):
                pipe.hgetall(self.KEY_TEMPLATE_ORDER % orderID)
            res = pipe.execute()
        return res

    def minPriceList(self):
        # in redis-py, pipeline is a transactional pipeline class by default
        with self.red.pipeline() as pipe:
            for orderID in self.red.lrange(self.KEY_TEMPLATE_ORDERS_BY_PRICE % self.minPrice(), 0, -1):
                pipe.hgetall(self.KEY_TEMPLATE_ORDER % orderID)
            res = pipe.execute()
        return res

