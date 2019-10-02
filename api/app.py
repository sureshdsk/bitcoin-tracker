import json
from flask import Flask, jsonify, url_for, render_template
from pyredis import Client
from datetime import datetime, timedelta
from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_LATEST_TRANSACTION_IDS_KEY, REDIS_TX_COUNTER_KEY_NS, REDIS_AGGREGATE_KEY_NS,\
    REDIS_AGGREGATE_SCORE_KEY_NS
from util import get_keys

app = Flask(__name__)
redis_client = Client(host=REDIS_HOST, port=REDIS_PORT, database=REDIS_DB)

@app.route('/')
def home():
    items = [
        {'title': 'Show Recent Transactions', 'link': url_for('show_transactions')},
        {'title': 'Show High Value Transactions', 'link': url_for('high_value_addr')},
        {'title': 'Show Transactions Per Minute', 'link': url_for('transactions_per_min', min_value=datetime.utcnow().strftime('%Y-%m-%d %H:%M'))}
    ]
    return render_template('home.html', items=items)

@app.route('/show_transactions', endpoint='show_transactions')
def get_recent_transactions():
    tx_index_list = redis_client.lrange(REDIS_LATEST_TRANSACTION_IDS_KEY, 0, 99)
    records = redis_client.mget(*tx_index_list)
    response = []
    for _record in records:
        response.append(json.loads(_record))
    return jsonify(response)

@app.route('/transactions_count_per_minute/<min_value>', endpoint='transactions_per_min')
def get_transactions_per_min(min_value):
    # http://0.0.0.0:5000/transactions_count_per_minute/2019-10-01 14:57

    transactions_count = []
    datetime_str = min_value + ':00'
    end_dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    start_dt = end_dt - timedelta(hours=1, minutes=1)
    dts = get_keys(start_dt, end_dt, timedelta(minutes=1), REDIS_TX_COUNTER_KEY_NS)
    records = redis_client.mget(*dts)

    for key, count in zip(dts, records):
        _min_value = key.split(':')[1]
        _min = '%s:%s' %(_min_value[:2], _min_value[2:])
        _count = 0
        if count:
            _count = int(count.decode('utf8'))
        transactions_count.append({'minute': _min, 'count': _count})
    return jsonify(transactions_count)


@app.route('/high_value_addr', endpoint='high_value_addr')
def get_transaction_aggregate():
    transaction_list = []
    utc_now = datetime.utcnow()
    expires_dt = utc_now - timedelta(hours=3, minutes=1)

    keys = get_keys(expires_dt, utc_now, timedelta(minutes=1), REDIS_AGGREGATE_KEY_NS)
    redis_client.zunionstore(REDIS_AGGREGATE_SCORE_KEY_NS, len(keys), *keys)

    records = redis_client.zrevrange(REDIS_AGGREGATE_SCORE_KEY_NS, 0, -1, 'WITHSCORES')
    for x in range(0, len(records), 2):
        transaction_list.append({'address': records[x].decode("utf-8"), 'value': int(records[x+1])})

    return jsonify(transaction_list)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)