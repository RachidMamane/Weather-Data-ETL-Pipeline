import sys , os 
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

"""print(f"DEBUG: DB_USER from .env: {os.getenv('DB_USER')}")
print(f"DEBUG: DB_PASSWORD from .env: {os.getenv('DB_PASSWORD')}")
print(f"DEBUG: DB_NAME from .env: {os.getenv('DB_NAME')}")
print(f"DEBUG: DB_HOST from .env: {os.getenv('DB_HOST')}")"""

from etl_pipeline.extract import extract_data
from etl_pipeline.transform import transform_data
from etl_pipeline.load import load_weather_data , create_weather_table_if_not_exists

def run_etl_pipeline(city:str) :
    print(f"\n--- Starting ETL for {city} ---")

    print(f"1. Extracting raw data for {city}...")
    raw_data = extract_data(city)
    if not raw_data:
        print(f"Extraction failed for {city}. Aborting ETL.")
        return

    print(f"2. Transforming raw data for {city}...")
    transformed_data = transform_data(raw_data)
    if not transformed_data:
        print(f"Transformation failed for {city}. Aborting ETL.")
        return
    print(f"3. Ensuring database table exists...")
    try:
        create_weather_table_if_not_exists()
    except Exception as e:
        print(f"Failed to create/ensure table existence: {e}. Aborting ETL.")
        return
    
    print(f"4. Loading transformed data for {city} into database...")
    load_weather_data(transformed_data)

    print(f"--- ETL for {city} complete ---")

if __name__ == "__main__":
    cities_to_process = [
        "Niamey",
        "Zinder",
        "Maradi",
        "Tahoua",
        "Agadez",
        "Arlit",
        "Dosso",
        "Birni N'Konni",
        "Tessaoua",
        "Gaya",
        "Diffa",
        "Tillabéri",
        "Dogondoutchi",
        "Abalak",
        "Mayahi",
        "Gouré",
        "Dakoro",
        "Nguigmi",
        "Ayorou",
        "Kollo"
    ]
    for city in cities_to_process:
        run_etl_pipeline(city)

    print("\nAll ETL processes finished for Niger cities.")
