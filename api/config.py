import json

BTC_WS_URL = 'wss://ws.blockchain.info/inv'

COUNTER_TOPIC = 'btctxcounter'
AGGREGATOR_TOPIC = 'btctxaggregator'
TRANSACTION_TOPIC = 'btctxdetails'

KAKFA_BS = 'localhost:9092'
KAFKA_AUTO_OFFSET_RESET= 'earliest'
KAFKA_CONSUMER_TIMEOUT_MS = 1000
KAFKA_VALUE_SERIALIZER = lambda v: json.dumps(v).encode('utf-8')
KAFKA_VALUE_DESERIALIZER = lambda v: json.loads(v, encoding='utf8')


REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

REDIS_AGGREGATE_KEY_NS = 'agg'
REDIS_AGGREGATE_SCORE_KEY_NS = 'agg'
REDIS_TX_KEY_NS = 'tx'
REDIS_TX_COUNTER_KEY_NS = 'tpm'

REDIS_LATEST_TRANSACTION_IDS_KEY = 'recent:tx'
REDIS_CACHE_EXPIRATION_HOURS = 3