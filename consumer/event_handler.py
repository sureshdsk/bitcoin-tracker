from pyredis import Client
from datetime import datetime, timedelta
import json
from config import REDIS_HOST, REDIS_AGGREGATE_KEY_NS, REDIS_TX_KEY_NS, REDIS_TX_COUNTER_KEY_NS, REDIS_CACHE_EXPIRATION_HOURS

redis_client = Client(host=REDIS_HOST)


class BitcoinBlock:
    def __init__(self, transaction_block):
        self.tx_block = transaction_block

        self.input = self.tx_block['x']['inputs'][-1]['prev_out']

    def get_from_address(self):
        return self.input['addr']

    def get_transaction_value(self):
        return self.input['value']

    def get_transaction_index(self):
        return self.tx_block['x']['tx_index']

    def get_transaction_timestamp(self):
        return self.tx_block['x']['time']

    def get_transaction_utc_datetime(self):
        return datetime.utcfromtimestamp(self.get_transaction_timestamp())

    def get_expiry_seconds(self):
        now_utc = datetime.utcnow()
        tx_datetime = self.get_transaction_utc_datetime()
        expires_at = tx_datetime + timedelta(hours=REDIS_CACHE_EXPIRATION_HOURS)
        return (now_utc - expires_at).seconds


def save_transaction_aggregate(tx_block):
    block = BitcoinBlock(tx_block)
    tx_from_addr = block.get_from_address()
    tx_value = block.get_transaction_value()
    tx_datetime = block.get_transaction_utc_datetime()
    expires_at = block.get_expiry_seconds()

    key = '%s:%s' % (REDIS_AGGREGATE_KEY_NS, tx_datetime.strftime('%H%M'))

    if redis_client.zrank(key, tx_from_addr) is None:
        redis_client.zadd(key, tx_value, tx_from_addr)
        redis_client.expire(key, expires_at)
    else:
        redis_client.zincrby(key, tx_value, tx_from_addr)


def save_transaction(tx_block):
    block = BitcoinBlock(tx_block)
    tx_from_addr = block.get_from_address()
    tx_value = block.get_transaction_value()
    tx_timestamp = block.get_transaction_timestamp()
    tx_index = block.get_transaction_index()
    expires_at = block.get_expiry_seconds()

    key = '%s:%s' % (REDIS_TX_KEY_NS, tx_index)
    redis_client.lpush('recent:tx', key)
    redis_client.set(key, json.dumps(dict(addr=tx_from_addr, value=tx_value, ts=tx_timestamp, idx=tx_index)))
    redis_client.expire(key, expires_at)


def update_transaction_counter(tx_block):
    block = BitcoinBlock(tx_block)
    expires_at = block.get_expiry_seconds()
    tx_datetime = block.get_transaction_utc_datetime()

    key = '%s:%s' % (REDIS_TX_COUNTER_KEY_NS, tx_datetime.strftime('%H%M'))
    if redis_client.exists(key):
        redis_client.incr(key)
    else:
        redis_client.set(key, 1)
        redis_client.expire(key, expires_at)

