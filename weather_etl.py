from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import pandas as pd
import json
import os


API_KEY = os.getenv("OPENWEATHER_API_KEY")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

# Helper function to convert Kelvin to Celsius
def kelvin_to_celsius(kelvin_temp):
    return kelvin_temp - 273.15


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    
    # Transformation Logic
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data['main']['temp'])
    feels_like_celsius = kelvin_to_celsius(data['main']['feels_like'])
    temp_min_celsius = kelvin_to_celsius(data['main']['temp_min'])
    temp_max_celsius = kelvin_to_celsius(data['main']['temp_max'])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (C)": temp_celsius,
        "Feels Like (C)": feels_like_celsius,
        "Min Temp (C)": temp_min_celsius,
        "Max Temp (C)": temp_max_celsius,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }

    # Convert to DataFrame
    df_data = pd.DataFrame([transformed_data])
    
    # Save to S3 (requires s3fs installed)
    aws_credentials = {
        "key": "YOUR_AWS_ACCESS_KEY",
        "secret": "YOUR_AWS_SECRET_KEY",
        "token": "YOUR_SESSION_TOKEN" # Optional: only if using temporary credentials
    }
    
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = f"current_weather_data_portland_{dt_string}.csv"
    
    # Replace 'your-bucket-name' with your actual S3 bucket
    df_data.to_csv(f"s3://your-bucket-name/{file_name}", index=False, storage_options=aws_credentials)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['youremailId@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='weather_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=f'data/2.5/weather?lat=17.3850&lon=78.4867&appid={API_KEY}'
    )

    # Extract weather data of Hyderabad using OpenWeatherMap API
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint=f'data/2.5/weather?lat=17.3850&lon=78.4867&appid={API_KEY}',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

  
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )

  
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
