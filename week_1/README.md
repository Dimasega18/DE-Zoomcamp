# Week 1 - Introduction to Data Engineering

## Overview
This week covers the basics of Data Engineering, including setting up the environment, ingesting data, and running queries in PostgreSQL.

## Key Topics
- Introduction to Data Engineering
- Setting up Docker & PostgreSQL
- Ingesting and querying data

## Quick Setup
1. **Clone the repository**
   ```bash
   git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git
   cd data-engineering-zoomcamp/week_1
   ```
2. **Run PostgreSQL**
   ```bash
   docker-compose up -d
   ```
3. **Download and load data**
   ```bash
   wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
   gunzip yellow_tripdata_2021-01.csv.gz
   python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_taxi --file_path=yellow_tripdata_2021-01.csv
   ```
4. **Run a simple query**
   ```sql
   SELECT COUNT(*) FROM yellow_taxi;
   ```

## Resources
- [DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [Docker Docs](https://docs.docker.com/)

