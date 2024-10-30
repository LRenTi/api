import json
import requests
import datetime
import os
import logging
from concurrent.futures import ThreadPoolExecutor

log_directory = 'logs/flylat'
log_file = f'{log_directory}/getCompanyData_error.log'

# Stelle sicher, dass das Verzeichnis für die Log-Datei existiert
os.makedirs(log_directory, exist_ok=True)

# Logging konfigurieren
logging.basicConfig(
    filename=log_file,
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def scrape_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error("Error fetching data from %s: %s", url, e)
        return None

def save_data(data_dict):
    for airline_id, data_info in data_dict.items():
        date_key = data_info['time'].strftime('%Y-%m-%d')
        daily_file_path = f'flylat/data/companydata/daily/{airline_id}.json'

        # Speichern der täglichen Daten
        existing_data = {}
        if os.path.exists(daily_file_path):
            with open(daily_file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)

        # Ersetze alle Einträge für diesen Tag
        existing_data[date_key] = [data_info['data']]
        
        # Speichern der täglichen Daten
        with open(daily_file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=4)

        # Überprüfen, ob es der letzte Tag des Monats ist
        if is_last_day_of_month(data_info['time']):
            monthly_file_path = f'flylat/data/companydata/monthly/{airline_id}.json'
            os.makedirs(os.path.dirname(monthly_file_path), exist_ok=True)

            # Speichern der monatlichen Daten
            monthly_existing_data = {}
            if os.path.exists(monthly_file_path):
                with open(monthly_file_path, 'r', encoding='utf-8') as f:
                    monthly_existing_data = json.load(f)

            # Ersetze alle Einträge für diesen Monat
            month_key = data_info['time'].strftime('%Y-%m')
            monthly_existing_data[month_key] = [data_info['data']]

            # Speichern der monatlichen Daten
            with open(monthly_file_path, 'w', encoding='utf-8') as f:
                json.dump(monthly_existing_data, f, indent=4)

def is_last_day_of_month(date):
    next_day = date + datetime.timedelta(days=1)
    return next_day.month != date.month

def main():
    utc_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=5)
    print("Collecting company data at", utc_time)
    airline_file = "flylat/data/airlines.json"
    os.makedirs('flylat/data/companydata/daily', exist_ok=True)
    
    with open(airline_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    data_dict = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {}
        for airline in data["airlines"]:
            airline_id = airline.get('id')
            url = f"https://flylat.net/company/get_data.php?id={airline_id}"
            futures[executor.submit(scrape_data, url)] = airline_id

        for future, airline_id in futures.items():  # Verwendung von .items() hier
            try:
                new_data = future.result()
                if new_data is not None:  # Überprüfe, ob die Daten erfolgreich abgerufen wurden
                    data_dict[airline_id] = {'data': new_data, 'time': utc_time}
            except requests.RequestException as e:
                logging.error(f"Request error processing data for {airline_id}: {e}")
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error processing data for {airline_id}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error processing data for {airline_id}: {e}")

    # Speichern der gesammelten Daten
    save_data(data_dict)

if __name__ == "__main__":
    main()
