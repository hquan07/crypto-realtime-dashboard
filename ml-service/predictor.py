#!/usr/bin/env python3
import os
import json
import time
import logging
from collections import deque

import redis
from kafka import KafkaConsumer
import numpy as np
import torch
import torch.nn as nn

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ml-predictor")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = "crypto-indicators"  # Using 1m aggregations from clickhouse/redis or fink?
# Wait, Flink now outputs 1-min aggregations to ClickHouse via JDBC. It doesn't output to crypto-indicators!
# In Phase 2, we removed Flink SQL which wrote to crypto-indicators.
# I should change predictor to read from 'crypto-trades' and aggregate itself, OR read 'crypto-alerts'.
# Actually, predictor can just read 'crypto-trades' directly to build its sequence.
TOPIC_TRADES = "crypto-trades"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Hyperparameters
SEQ_LEN = 10  # lookback 10 trades

class PriceLSTM(nn.Module):
    def __init__(self, input_size=1, hidden_size=16, num_layers=1):
        super(PriceLSTM, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])
        return out

def main():
    logger.info("Initializing PyTorch LSTM Predictor...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Initialize model (dummy weights for now, in a real app this would load from a .pth file)
    model = PriceLSTM()
    model.eval()

    # Track recent prices per symbol
    history = {}

    consumer = KafkaConsumer(
        TOPIC_TRADES,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

    logger.info("Listening to %s for ML predictions...", TOPIC_TRADES)

    for message in consumer:
        try:
            trade = message.value
            sym = trade.get('symbol')
            price = float(trade.get('price', 0.0))
            if not sym or not price:
                continue

            if sym not in history:
                history[sym] = deque(maxlen=SEQ_LEN)
            history[sym].append(price)

            if len(history[sym]) == SEQ_LEN:
                # Prepare tensor: shape (batch=1, seq=SEQ_LEN, features=1)
                seq = np.array(history[sym]).reshape(1, SEQ_LEN, 1).astype(np.float32)
                
                # Normalize (min-max scaling on current window)
                min_p, max_p = seq.min(), seq.max()
                if max_p > min_p:
                    seq_norm = (seq - min_p) / (max_p - min_p)
                else:
                    seq_norm = seq - min_p
                
                tensor = torch.tensor(seq_norm)
                
                # Inference
                with torch.no_grad():
                    pred_norm = model(tensor).item()
                
                # Denormalize
                if max_p > min_p:
                    pred_price = pred_norm * (max_p - min_p) + min_p
                else:
                    pred_price = price
                
                trend = "UP" if pred_price > price else "DOWN"
                
                # Save to Redis
                r.hset("crypto:predictions", sym, json.dumps({
                    "predicted_price": round(pred_price, 4),
                    "current_price": price,
                    "trend": trend,
                    "timestamp": time.time()
                }))
                
        except Exception as e:
            logger.error("Error in ML pipeline: %s", e)

if __name__ == '__main__':
    main()