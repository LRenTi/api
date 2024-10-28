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
        file_path = f'flylat/data/companyData/daily/{airline_id}.json'

        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        else:
            existing_data = {}

        # Ersetze alle Einträge für diesen Tag
        existing_data[date_key] = [data_info['data']]

        # Speichern der aktualisierten Daten in der JSON-Datei
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=4)

def main():
    utc_time = datetime.datetime.now(datetime.timezone.utc)
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

        for future in futures:
            airline_id = futures[future]
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
