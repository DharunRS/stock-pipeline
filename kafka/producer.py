import json, time, random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
BASE_PRICES = {'AAPL': 180, 'GOOGL': 140, 'MSFT': 380, 'TSLA': 250, 'AMZN': 175}

def generate_tick(symbol):
    price = BASE_PRICES[symbol] * (1 + random.uniform(-0.002, 0.002))
    return {
        'symbol':    symbol,
        'price':     round(price, 4),
        'volume':    random.randint(100, 10000),
        'timestamp': datetime.utcnow().isoformat(),
        'bid':       round(price - random.uniform(0.01, 0.05), 4),
        'ask':       round(price + random.uniform(0.01, 0.05), 4)
    }

if __name__ == '__main__':
    print("Producer started — streaming tick data...")
    tick_count = 0
    while True:
        for symbol in SYMBOLS:
            tick = generate_tick(symbol)
            producer.send('stock-ticks', key=symbol.encode(), value=tick)
            BASE_PRICES[symbol] = tick['price']
            tick_count += 1
        if tick_count % 100 == 0:
            print(f"Sent {tick_count} ticks")
        time.sleep(0.1)