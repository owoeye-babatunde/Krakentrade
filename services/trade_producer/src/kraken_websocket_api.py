import json
from typing import List
from websocket import create_connection
from loguru import logger
class KrakenWebsocketAPI:
    """
    Class for reading real-time trades from the Kraken Websocket API
    """
    
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        """
        Initialize the KrakenWebsocketAPI instance

        Args:
            product_id (str): The product id to get the trades from
        """


        self.product_id = product_id


        # establish connection to the Kraken websocket API
        self._ws = create_connection(self.URL)
        logger.debug('Connection established')

        # subscribe to the trades for the given `product_id`
        self._subscribe(product_id)

    def get_trades(self) -> List[dict]:
        """
        Returns the latest batch of trades from the Kraken Websocket API
        """
        message = self._ws.recv()

        if 'heartbeat' in message:
            # when I get a heartbeat, I return an empty list
            print(message)
            return []

        # parse the message string as a dictionary
        message = json.loads(message)

        # extract trades from the message['data'] field
        trades = []
        for trade in message['data']:

            breakpoint()

        return trades
    
    
    def is_done(self) -> bool:
        """
        Returns True if the Kraken Websocket API is closed
        """
        False

    def _subscribe(self, product_id: str):
        """
        Establish connection to the Kraken websocket API and subscribe to the trades for the given `product_id`.
        """
        logger.info(f'Subscribing to trades for {product_id}')
        # let's subscribe to the trades for the given `product_id`
        msg = {
            'method': 'subscribe',
            'params': {
                'channel': 'trade',
                'symbol': [product_id],
                'snapshot': False,
            },
        }
        self._ws.send(json.dumps(msg))
        logger.info('Subscription worked!')

        # For each product_id we dump
        # the first 2 messages we got from the websocket, because they contain
        # no trade data, just confirmation on their end that the subscription was successful
        for product_id in [product_id]:
            _ = self._ws.recv()
            _ = self._ws.recv()


