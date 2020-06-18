"""

@author: Zhishe

"""

from time import sleep

import redis
from bitmex_websocket import BitMEXWebsocket

from bitmex_config import API_KEY, API_SECRET

DB_NUM = 3  # Redis database number


def run():

    red = redis.StrictRedis(charset='utf-8', decode_responses=True, db=DB_NUM)

    # instantiate the WS will make it connect.
    ws = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD", red=red,
                         api_key=API_KEY, api_secret=API_SECRET)

    #logger.info('Instrument data: %s' % ws.get_instrument())

    # run forever
    while ws.ws.sock.connected:
        #logger.info("Ticker: %s" % ws.get_ticker())
        #if ws.api_key:
        #    logger.info("Funds: %s" % ws.funds())
        #logger.info("Market Depth: %s" % ws.market_depth())
        #logger.info("Recent Trades: %s\n\n" % ws.recent_trades())
        sleep(10)


if __name__ == '__main__':
    run()
