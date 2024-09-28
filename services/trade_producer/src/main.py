from loguru import logger
from quixstreams import Application
from src. kraken_websocket_api import KrakenWebsocketAPI


def produce_trades(
        kafka_broker_address: str,
        kafka_topic: str,
        product_id: str,
):
    """

    Reads trades from the kraken Websocket API and saves them in the given 'kafka topic'

    Args:
        kafka_broker_address (str): The address of the kafka broker
        kafka_topic (str): The Kafka topic to save the trades
        product_id (str): The product id to get the trades from

    Returns:
        None
    
    """
    # Create an Application:
    app = Application(broker_address=kafka_broker_address)

    # Define a Topic
    topic = app.topic(name=kafka_topic, value_serializer='json')

    # Create a Kraken API object
    kraken_api = KrakenWebsocketAPI(product_id=product_id)

    # Create a Producer instance
    with app.get_producer() as producer:
        while True:

            trades = kraken_api.get_trades()

            for trade in trades:


                # Serialize an event using the defined topic
                # Transform it into a sequence of bytes
                message = topic.serialize(key=trade["product_id"], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.debug(f"Pushed trade to Kafka: {trade}")





if __name__ == "__main__":

    produce_trades(
        kafka_broker_address="localhost:19092",
        kafka_topic="trades",
        product_id="Eth/USD"
    )


