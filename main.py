"""

@author: Zhishe

"""

import logging
from time import sleep

from bitmex_websocket import BitMEXWebsocket

from api_config import API_KEY, API_SECRET

DIR = '/Users/zhishe/myProjects/bitmex/orderbook_dataCollector'

def run():
    logger = setup_logger()

    # instantiate the WS will make it connect.
    ws = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=API_KEY, api_secret=API_SECRET)

    logger.info('Instrument data: %s' % ws.get_instrument())

    # run forever
    while ws.ws.sock.connected:
        #logger.info("Ticker: %s" % ws.get_ticker())
        #if ws.api_key:
        #    logger.info("Funds: %s" % ws.funds())
        #logger.info("Market Depth: %s" % ws.market_depth())
        #logger.info("Recent Trades: %s\n\n" % ws.recent_trades())
        sleep(10)


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("\n\n%(asctime)s - %(name)s.%(funcName)s - %(levelname)s - %(message)s")

    # print INFO level logs to terminal
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    # write DEBUG level logs to a file
    fh = logging.FileHandler(DIR + '/log.log')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    return logger


if __name__ == '__main__':
    run()
