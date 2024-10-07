from typing import List
from quixstreams import Application
from loguru import logger
from src.hopsworks_api import push_value_to_feature_group


def topic_to_feature_store(
        kafka_broker_address: str,
        kafka_input_topic: str,
        kafka_consumer_group: str,
        feature_group_name: str,
        feature_group_version: int,
        feature_group_primary_keys: List[str],
        feature_group_event_time: str,
        start_offline_materialization: bool,
        # we will probably need some feature store credentials here
):
    """
    Reads incoming messages from kafka_input_topic and pushes the
    feature store.

    Args:
        kafka_broker_address (str): Kafka broker address
        kafka_input_topic (str): Kafka input topic
        kafka_consumer_group (str): Kafka consumer group
        feature_group_name (str): Feature group name
        feature_group_version (int): Feature group version
        feature_group_primary_keys (List[str]): Feature group primary keys
        feature_group_event_time (str): Feature group event time
        start_offline_materialization (bool): Whether to start offline 
            materialization of the feature group or not when we save the 'value' to the feature group

    Returns:
        None
    """

    app = Application(
        broker_address=kafka_broker_address,
        #auto_offset_reset="earliest",
        consumer_group=kafka_consumer_group,
    )

    # create a consumer and start pulling loop
    with app.get_consumer() as consumer:

        # subscribe to the input topic
        consumer.subscribe(topics=[kafka_input_topic])

        while True:

            msg = consumer.poll(0.1)

            if msg is None:
                continue

            elif msg.error():
                logger.error('kafka error', msg.error())
                continue

            value = msg.value()

            #decode the message into a dictionary
            import json
            value = json.loads(value.decode('utf-8'))

            push_value_to_feature_group(value, 
                                        feature_group_name, 
                                        feature_group_version,
                                        feature_group_primary_keys,
                                        feature_group_event_time,
                                        start_offline_materialization,

            )

            #breakpoint()

            # we need to push values to the featrure store
            
            # store the offset of the processed message on the consumer
            # for the auto commit nachanism,
            # it will send it to kafka in the background
            #storing offset only after the message is processed enables at-least-once processing
            # delivery guarantees.
            consumer.store_offsets(message=msg)

if __name__ == "__main__":

    from src.config import config

    topic_to_feature_store(
        kafka_broker_address=config.kafka_broker_address,
        kafka_input_topic=config.kafka_input_topic,
        kafka_consumer_group=config.kafka_consumer_group,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        feature_group_primary_keys=config.feature_group_primary_keys,
        feature_group_event_time=config.feature_group_event_time,
        start_offline_materialization=config.start_offline_materialization,
    )
