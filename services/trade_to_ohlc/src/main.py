from typing import Any, List, Optional, Tuple
from quixstreams import Application
from loguru import logger
from datetime import timedelta

def init_ohlcv_candle(
        trade: dict,
):
    """
    Returns the OHLCV candle when the first trade message is received.

    Args:
        trade (dict): Trade message

    Returns:
        dict: OHLCV candle
    """
    return {
        'open': trade['price'],
        'high': trade['price'],
        'low': trade['price'],
        'close': trade['price'],
        'volume': trade['quantity'],
        'product_id': trade['product_id'],
        
    }

def update_ohlcv_candle(candle: dict, trade: dict):
    """
    Updates the OHLCV candle with the new trade message.

    Args:
        candle (dict): OHLCV candle
        trade (dict): Trade message

    Returns:
        dict: Updated OHLCV candle
    """
    candle['high'] = max(candle['high'], trade['price'])
    candle['low'] = min(candle['low'], trade['price'])
    candle['close'] = trade['price']
    candle['volume'] += trade['quantity']
    candle['product_id'] = trade['product_id']

    return candle

def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type#: TimestampType,
    ) -> int:
    """
    Specifying a custom timestamp extractor to use the timestamp from the message payload 
    instead of Kafka timestamp.

    Extracts the field where the timestamp is stored in the message payload.
    """
    return value["timestamp_ms"]


def transform_trade_to_ohlcv(
        kafka_broker_address: str,
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_consumer_group: str,
        ohlcv_window_seconds:int
):
    """
    Reads incoming trade messages from a Kafka topic, 
    aggregates them into OHLC bars and writes the bars 
    to another Kafka topic.

    Args:
        kafka_broker_address (str): Kafka broker address
        kafka_input_topic (str): Kafka input topic
        kafka_output_topic (str): Kafka output topic
        kafka_consumer_group (str): Kafka consumer group

    Returns:
        None
    """



    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
    )

    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json', timestamp_extractor=custom_ts_extractor)
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # create quixstreams dataframe
    sdf=app.dataframe(input_topic)

    
    sdf=(
        sdf.tumbling_window(duration_ms=timedelta(seconds=ohlcv_window_seconds))
        .reduce(reducer=update_ohlcv_candle, initializer=init_ohlcv_candle)
        .final()
        #.current()
    )

    # check if we are reading incoming trades
    

    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['volume'] = sdf['value']['volume']
    sdf['product_id'] = sdf['value']['product_id']
    sdf['timestamp_ms'] = sdf['end']

    
    # keep only the columns we need
    sdf = sdf[['product_id', 'timestamp_ms', 'open', 'high', 'low', 'close', 'volume']]

    # log the data
    sdf.update(logger.debug)

    # push the data to the output topic
    sdf = sdf.to_topic(output_topic)

    
    app.run(sdf)

if __name__ == '__main__':

    from src.config import config

    transform_trade_to_ohlcv(
        kafka_broker_address=config.kafka_broker_address,
        kafka_input_topic=config.kafka_input_topic,
        kafka_output_topic=config.kafka_output_topic,
        kafka_consumer_group=config.kafka_consumer_group,
        ohlcv_window_seconds=config.ohlcv_window_seconds,
    )


    
