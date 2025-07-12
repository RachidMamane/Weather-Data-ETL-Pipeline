from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, DateTime
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]) :
    raise ValueError("Database connection details not found in environment variables.Check your env variables")
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@"
    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
engine = create_engine(DATABASE_URL)
metadata = MetaData()


weather_table = Table(
    'weather_table', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column("Datetime", DateTime),
    Column('City_name', String),
    Column('Latitude', Float),
    Column('Longitude', Float),
    Column('main', String),
    Column('description', String),
    Column('temperature', Float),
    Column('feels_like', Float),
    Column('min_temperature', Float),
    Column('max_temperature', Float),
    Column('pressure', Integer),
    Column('humidity', Integer),
    Column('visibility', Integer),
    Column('wind_speed', Float),
    Column('wind_deg', Integer),
    Column('sunrise', DateTime),
    Column('sunset', DateTime),
    extend_existing=True
)

def create_weather_table_if_not_exists(): 
    try:
        metadata.create_all(engine)
        print("Table 'weather_table' ensured to exist.")
    except SQLAlchemyError as e:
        print(f"Error creating table: {e}")
        raise

def load_weather_data(weather_data: dict):
    if not weather_data:
        print("No data to load.")
        return

    try:
         
        """print(f"DEBUG IN LOAD: Type of weather_data: {type(weather_data)}")
        print(f"DEBUG IN LOAD: Content of weather_data: {weather_data}")"""
        with engine.connect() as connection:
            connection.execute(weather_table.insert().values(**weather_data))
            connection.commit()
        print(f"Data for {weather_data.get('City_name', 'N/A')} inserted successfully!")
    except SQLAlchemyError as e:
        print(f"Database error occurred during data insertion: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during data loading: {e}")