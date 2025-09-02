FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends curl git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt && python -m playwright install --with-deps chromium
COPY . .
CMD ["python","-m","scrapers.geo.sweep","--headless","--order","density","--panpoints","data/panpoints/airbnb_panpoints_gdl_zap.csv"]
