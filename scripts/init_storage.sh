#!/bin/bash

echo "Initializing Lakehouse storage structure..."

mkdir -p data/bronze/orders
mkdir -p data/silver/orders
mkdir -p data/silver/quarantine_orders
mkdir -p data/gold/revenue_by_city_hour
mkdir -p data/gold/revenue_by_product_day
mkdir -p data/checkpoints

touch data/bronze/.gitkeep
touch data/silver/.gitkeep
touch data/gold/.gitkeep
touch data/checkpoints/.gitkeep

echo "Lakehouse directories created successfully."
