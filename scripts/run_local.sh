#!/usr/bin/env bash
set -euo pipefail
python -m pip install -r requirements.txt
python -m playwright install --with-deps chromium
python -m scrapers.geo.sweep --headless --order density --panpoints data/panpoints/airbnb_panpoints_gdl_zap.csv
