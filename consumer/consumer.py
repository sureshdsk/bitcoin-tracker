#!/usr/bin/env python
import logging
import multiprocessing
from config import COUNTER_TOPIC, AGGREGATOR_TOPIC, TRANSACTION_TOPIC, KAKFA_BS, KAFKA_AUTO_OFFSET_RESET, KAFKA_CONSUMER_TIMEOUT_MS, KAFKA_VALUE_DESERIALIZER
from kafka import KafkaConsumer
from event_handler import update_transaction_counter, save_transaction, save_transaction_aggregate

consumer_methods = {
    COUNTER_TOPIC: update_transaction_counter,
    AGGREGATOR_TOPIC: save_transaction_aggregate,
    TRANSACTION_TOPIC: save_transaction,
}


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=KAKFA_BS,
                                 auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
                                 consumer_timeout_ms=KAFKA_CONSUMER_TIMEOUT_MS,
                                 value_deserializer=KAFKA_VALUE_DESERIALIZER)
        consumer.subscribe([COUNTER_TOPIC, AGGREGATOR_TOPIC, TRANSACTION_TOPIC])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message.topic, message.value)


                consumer_methods[message.topic](message.value)

                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    consumer = Consumer()
    consumer.start()
    consumer.join()

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
