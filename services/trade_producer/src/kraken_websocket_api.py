from typing import List
from websocket import create_connection

class KrakenWebsocketAPI:
    """
    Class for reading real-time trades from the Kraken Websocket API
    """

    def __init__(self, product_id: str):
        """
        Initialize the KrakenWebsocketAPI instance

        Args:
            product_id (str): The product id to get the trades from
        """


        self.product_id = product_id

    def get_trades(self) -> List[dict]:
        """
        Returns the latest batch of trades from the Kraken Websocket API
        """
        event = [{
            "product_id": "Eth/USD",
            "price": 1000,
            "qty": 1,
            "timestamp_ms": 1630000000000,
        }]

        from time import sleep
        sleep(1)

        return event
    
    
def is_done(self) -> bool:
    """
    Returns True if the Kraken Websocket API is closed
    """
    False


