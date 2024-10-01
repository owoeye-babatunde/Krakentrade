from quixstreams import Application
from datetime import datetime, timedelta
from loguru import logger

def init_ohlcv_candle(trade: dict):
    """
    Returns the initial OHLCV candle when the first 'trade' in that window is happens.
    """
    return {
        'timestamp': trade['timestamp'],
        'open': trade['price'],
        'high': trade['price'],
        'low': trade['price'],
        'close': trade['price'],
        'volume': trade['quantity'],
    }

def update_ohlcv_candle(candle: dict, trade: dict):
    """
    Updates the given 'ohlcv_candle' with the new 'trade'.
    """
    candle['high'] = max(candle['high'], trade['price'])
    candle['low'] = min(candle['low'], trade['price'])
    candle['close'] = trade['price']
    candle['volume'] += trade['quantity']


def transform_trade_to_ohlcv(
        kafka_broker_address: str,
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_consumer_group: str,  
):
    """
    Reads incoming trades from the given 'kafka_input_topic', transforms them into OHLC data 
    and outputes them to the given 'kafka_output_topic'.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_input_topic (str): The topic to read trades from.
        kafka_output_topic (str): The topic to save the OHLC data.
        kafka_consumer_group (str): The consumer group.

    Returns:
        None
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
    )

    # Create an input and output topics
    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # Create a Quix Streams DataFrame
    sdf = app.dataframe(input_topic)

    sdf = (
        sdf.tumbling_window(duration_ms=timedelta(seconds=1))
        .reduce(reducer=update_ohlcv_candle, initializer=init_ohlcv_candle)
        .final()
    )

    # print the output to the console
    sdf.apply(logger)

    app.run(sdf)

if __name__ == "__main__":
    transform_trade_to_ohlcv(
        kafka_broker_address='localhost:19092',
        kafka_input_topic='trades',
        kafka_output_topic='ohlcv',
        kafka_consumer_group='consumer_group_trade_to_ohlcv',
    )