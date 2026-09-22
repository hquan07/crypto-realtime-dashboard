#!/usr/bin/env python3
import time
import json
import os
import logging
import feedparser
from kafka import KafkaProducer
from transformers import pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("sentiment-pipeline")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_SENTIMENT = "crypto-sentiment"

# RSS Feed for Crypto News
RSS_URL = "https://cointelegraph.com/rss"

def main():
    logger.info("Initializing FinBERT Sentiment Pipeline...")
    
    # Initialize FinBERT
    # Using ProsusAI/finbert which is specialized for financial sentiment
    try:
        sentiment_pipeline = pipeline("sentiment-analysis", model="ProsusAI/finbert")
        logger.info("FinBERT model loaded successfully.")
    except Exception as e:
        logger.error("Failed to load FinBERT: %s", e)
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    seen_entries = set()

    while True:
        try:
            logger.info("Scraping news from %s", RSS_URL)
            feed = feedparser.parse(RSS_URL)
            
            new_articles = 0
            for entry in feed.entries:
                if entry.id not in seen_entries:
                    seen_entries.add(entry.id)
                    title = entry.title
                    
                    # Analyze sentiment of the title
                    result = sentiment_pipeline(title)[0]
                    label = result['label']
                    score = result['score']
                    
                    # Convert to numeric score (-1 to 1)
                    if label == 'positive':
                        num_score = score
                    elif label == 'negative':
                        num_score = -score
                    else:
                        num_score = 0.0
                        
                    msg = {
                        "source": "cointelegraph",
                        "title": title,
                        "sentiment_label": label,
                        "sentiment_score": float(num_score),
                        "confidence": float(score),
                        "timestamp": time.time()
                    }
                    
                    producer.send(TOPIC_SENTIMENT, msg)
                    new_articles += 1
            
            if new_articles > 0:
                producer.flush()
                logger.info("Processed and sent %d new articles.", new_articles)
                
        except Exception as e:
            logger.error("Error in sentiment pipeline: %s", e)
            
        # Wait 5 minutes before polling RSS again
        time.sleep(300)

if __name__ == '__main__':
    main()
