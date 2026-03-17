# Weather Data ETL Pipeline: Airflow, Python & AWS S3

A robust ETL (Extract, Transform, Load) pipeline that automates the collection of real-time weather data for Hyderabad, India, using the OpenWeatherMap API and stores it as structured data in an Amazon S3 bucket.

## Project Architecture

The pipeline follows a standard ETL pattern:

1.  **Extract**: Verifies API availability with `HttpSensor` and pulls live JSON data from **OpenWeatherMap API** via `SimpleHttpOperator`.
2.  **Transform**: Uses **Pandas** to:
    - Convert temperature from Kelvin to Celsius.
    - Parse Unix timestamps into localized date-time objects.
    - Flatten nested JSON into a structured tabular format.
3.  **Load**: Saves the processed data as a `.csv` file and uploads it to an **AWS S3** bucket using `s3fs`.

## Setup & Installation

### 1. Prerequisites

1.1AWS Account (S3 Bucket & EC2 instance)

1.2 OpenWeatherMap API Key

### 2. Environment Variables

2.1 To keep credentials secure, create a .env file in your project root:

### 3. Installation

Clone the repository

```bash
    git clone https://github.com/SangitaSingha073/ETL_Weather_Data.git

    pip install -r requirements.txt
```

## AWS Connection

This project is optimized to run on an **EC2 instance** with an IAM Role. Ensure your EC2 has the `AmazonS3FullAccess` policy attached to avoid hardcoding AWS credentials.
