#!/bin/bash
# Start both the scraper and the Flask API in parallel

echo "Starting MatterTracker services..."

# Start the scraper in the background
python scraper.py &
SCRAPER_PID=$!
echo "Scraper started (PID: $SCRAPER_PID)"

# Start the Flask API in the foreground (Railway needs this)
echo "Starting Flask API on port ${PORT:-8080}..."
gunicorn api:app --bind 0.0.0.0:${PORT:-8080} --workers 1 --timeout 60

# If gunicorn exits, kill the scraper too
kill $SCRAPER_PID
