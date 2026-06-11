import json
from kafka import KafkaConsumer
from collections import defaultdict

consumer = KafkaConsumer(
    'stock-ticks',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='stock-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

stats = defaultdict(lambda: {'count': 0, 'total_volume': 0, 'prices': []})

print("Consumer started — reading tick data...")
for message in consumer:
    tick = message.value
    symbol = tick['symbol']
    s = stats[symbol]
    s['count'] += 1
    s['total_volume'] += tick['volume']
    s['prices'].append(tick['price'])

    if s['count'] % 50 == 0:
        avg = sum(s['prices'][-50:]) / 50
        print(f"{symbol} | count={s['count']} | avg_price={avg:.2f} | vol={s['total_volume']}")