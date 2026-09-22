#!/bin/bash
echo "Starting Sentiment Scraper in background..."
python3 -u sentiment_scraper.py &

echo "Starting LSTM Predictor..."
python3 -u predictor.py
