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
        existing_data[date_key] = {date_key: data_info['data']}
        
        # Speichern der täglichen Daten
        with open(daily_file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=4)

        # Überprüfen, ob es der letzte Tag des Monats ist
        if is_last_day_of_month(data_info['time']):
            monthly_file_path = f'flylat/data/companydata/monthly/{airline_id}.json'
            os.makedirs(os.path.dirname(monthly_file_path), exist_ok=True)

            # Speichern der monatlichen Daten
            monthly_existing_data = []
            if os.path.exists(monthly_file_path):
                with open(monthly_file_path, 'r', encoding='utf-8') as f:
                    monthly_existing_data = json.load(f)

            # Monatseintrag als Dictionary hinzufügen oder ersetzen
            month_key = data_info['time'].strftime('%Y-%m')
            month_entry = {month_key: data_info['data']}

            # Überprüfen, ob der Monat bereits existiert, um ihn zu ersetzen
            month_exists = False
            for i, entry in enumerate(monthly_existing_data):
                if month_key in entry:
                    monthly_existing_data[i] = month_entry  # Ersetze den bestehenden Monatseintrag
                    month_exists = True
                    break

            if not month_exists:
                # Füge den neuen Monatseintrag hinzu, wenn er noch nicht existiert
                monthly_existing_data.append(month_entry)

            # Speichern der monatlichen Daten als Liste
            with open(monthly_file_path, 'w', encoding='utf-8') as f:
                json.dump(monthly_existing_data, f, indent=4)

def is_last_day_of_month(date):
    next_day = date + datetime.timedelta(days=1)
    return next_day.month != date.month

def main():
    utc_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=12)
    print("Collecting company data for", utc_time.strftime('%Y-%m-%d %H:%M:%S'))
    airline_file = "flylat/data/airlines.json"
    
    # Öffnen der Airline-Datei und Laden der Daten als Liste
    with open(airline_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Sicherstellen, dass `data` eine Liste ist
    if not isinstance(data, list):
        logging.error("Expected a list of airlines in JSON file.")
        return
    
    data_dict = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {}
        for airline in data:
            airline_id = airline.get('id')
            url = f"https://flylat.net/company/get_data.php?id={airline_id}"
            futures[executor.submit(scrape_data, url)] = airline_id

        for future in futures:
            airline_id = futures[future]
            try:
                new_data = future.result()
                if new_data:
                    data_dict[airline_id] = {'data': new_data, 'time': utc_time}
            except Exception as e:
                logging.error("Error processing data for %s: %s", airline_id, e)

    # Speichern der gesammelten Daten
    save_data(data_dict)

if __name__ == "__main__":
    main()

