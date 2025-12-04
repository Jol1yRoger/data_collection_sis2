# OLX Phone Ads ETL Pipeline

## Project Overview

This project implements a mini end–to–end data pipeline “from website to database”:

1. Scraping phone advertisements from a dynamic OLX section using Playwright (JavaScript–rendered content).
2. Cleaning and preprocessing the collected data with pandas.
3. Storing the processed dataset into a SQLite database (`olx_data.db`).
4. Automating the whole workflow with Apache Airflow on a daily schedule (no more than once per 24 hours).

The pipeline demonstrates data extraction, cleaning, and orchestration concepts.

## Website Description

We scrape the phone and accessories section of OLX Kazakhstan:

- Base URL: `https://www.olx.kz/elektronika/telefony-i-aksesuary/`
- The product list is rendered in the browser and uses dynamic content loading (JavaScript).
- Pagination is handled via the `?page=` query parameter.

For each ad we collect:

- `title`
- `raw_price`
- `raw_location_date` (location and posting time in one string)
- `link` to the ad

After cleaning, the final dataset contains at least 100 rows.

## Project Structure

Example structure of the repository:

```text
project/
  README.md
  requirements.txt
  olx_etl_pipeline.py
  olx_dag.py
  olx_data.db


