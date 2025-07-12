import datetime


def _kelvin_to_celcius(temperature_k : float) -> float : 
  celcius = temperature_k - 273.15
  return round(celcius ,2 )

def transform_data(data: dict) -> dict | None :
    if not data:
        print("No raw data provided for transformation.")
        return None

    try : 
        current_time = datetime.datetime.utcfromtimestamp(data['dt'])
        city_name = data['name']
        latitude = data['coord']['lat']
        longitude = data['coord']['lon']
        main_weather = data['weather'][0]['main']
        description = data['weather'][0]['description']
        temperature = _kelvin_to_celcius(data['main']['temp'])
        feels_like_temp = _kelvin_to_celcius(data['main']['feels_like'])
        min_temperature = _kelvin_to_celcius(data['main']['temp_min'])
        max_temperature = _kelvin_to_celcius(data['main']['temp_max'])
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        visibility = data.get('visibility', None)
        wind_speed = data['wind']['speed']
        wind_deg = data['wind']['deg']
        sunrise = datetime.datetime.utcfromtimestamp(data['sys']['sunrise'])
        sunset = datetime.datetime.utcfromtimestamp(data['sys']['sunset'])

        transformed_weather_data = {
            "Datetime": current_time,
            "City_name": city_name,
            "Latitude": latitude,
            "Longitude": longitude,
            "main": main_weather,
            "description": description,
            "temperature": temperature,
            "feels_like": feels_like_temp,
            "min_temperature": min_temperature,
            "max_temperature": max_temperature,
            "pressure": pressure,
            "humidity": humidity,
            "visibility": visibility,
            "wind_speed": wind_speed,
            "wind_deg": wind_deg,
            "sunrise": sunrise,
            "sunset": sunset,
        }
        return transformed_weather_data
    except KeyError as e:
        print(f"Missing expected data key during transformation: {e}. Raw data: {data}")
    except Exception as e:
        print(f"An unexpected error occurred during data transformation: {e}. Raw data: {data}")
        return None