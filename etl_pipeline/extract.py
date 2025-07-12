import requests 
import pandas as pd
import numpy as np
import os 
from dotenv import load_dotenv

load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

def extract_data(city:str) -> dict | None :
    if not OPENWEATHER_API_KEY:
        print("Error: OPENWEATHER_API_KEY not found in environment variables. Please check your .env file.")
        return None
    params = {
        "q": city,
        "appid": OPENWEATHER_API_KEY
    }
    try :
        response = requests.get(OPENWEATHER_BASE_URL , params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e :
        print(f"Error fetching data for {city}: HTTP Error {e.response.status_code} - {e.response.text}")
    except requests.exceptions.ConnectionError as e :
        print(f"Error fetching data for {city}: Connection Error - {e}")
    except requests.exceptions.Timeout as e :
        print(f"Error fetching data for {city}: Timeout Error - {e}")
    except requests.exceptions.RequestException as e : 
        print(f"An unexpected error occurred while fetching data for {city}: {e}")
    return None    

