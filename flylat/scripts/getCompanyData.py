import json
import requests
import datetime
import os
import logging
from concurrent.futures import ThreadPoolExecutor

# Verzeichnisse für Logs und Daten
log_directory = 'logs/flylat'
data_directory = 'flylat/data/companydata'

# Sicherstellen, dass Verzeichnisse für Logs und Daten existieren
os.makedirs(log_directory, exist_ok=True)
os.makedirs(f'{data_directory}/daily', exist_ok=True)
os.makedirs(f'{data_directory}/monthly', exist_ok=True)

# Logging-Konfiguration
log_file = f'{log_directory}/getCompanyData_error.log'
logging.basicConfig(
    filename=log_file,
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def scrape_data(url):
    """Daten von der angegebenen URL abrufen."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error("Fehler beim Abrufen der Daten von %s: %s", url, e)
        return None

def save_data(data_dict):
    """Speichert die gesammelten Daten als Liste in JSON-Dateien."""
    for airline_id, data_info in data_dict.items():
        date_entry = {
            "date": data_info['time'].strftime('%Y-%m-%d'),
            "data": data_info['data']
        }

        # Tägliche Daten als Liste speichern
        daily_file_path = f'{data_directory}/daily/{airline_id}.json'
        daily_data = []
        if os.path.exists(daily_file_path):
            with open(daily_file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                if isinstance(existing_data, list):
                    daily_data = existing_data
                else:
                    daily_data = [existing_data]  # Falls ein Dictionary vorliegt, in eine Liste umwandeln
        
        daily_data.append(date_entry)  # Hinzufügen des neuen Eintrags
        with open(daily_file_path, 'w', encoding='utf-8') as f:
            json.dump(daily_data, f, indent=4)

        # Überprüfen, ob es der letzte Tag des Monats ist
        if is_last_day_of_month(data_info['time']):
            # Monatliche Daten als Liste speichern
            monthly_file_path = f'{data_directory}/monthly/{airline_id}.json'
            monthly_data = []
            if os.path.exists(monthly_file_path):
                with open(monthly_file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    if isinstance(existing_data, list):
                        monthly_data = existing_data
                    else:
                        monthly_data = [existing_data]  # Falls ein Dictionary vorliegt, in eine Liste umwandeln

            monthly_data.append(date_entry)  # Hinzufügen des neuen Monatseintrags
            with open(monthly_file_path, 'w', encoding='utf-8') as f:
                json.dump(monthly_data, f, indent=4)

def is_last_day_of_month(date):
    """Überprüfen, ob das Datum der letzte Tag des Monats ist."""
    next_day = date + datetime.timedelta(days=1)
    return next_day.month != date.month

def main():
    """Hauptprogramm für das Sammeln und Speichern der Unternehmensdaten."""
    utc_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=12)
    print("Sammle Unternehmensdaten für", utc_time.strftime('%Y-%m-%d %H:%M:%S'))
    airline_file = "flylat/data/airlines.json"
    
    # Airline-Daten aus der Datei laden
    with open(airline_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Sicherstellen, dass die Daten als Liste geladen wurden
    if not isinstance(data, list):
        logging.error("Erwartet eine Liste von Airlines in der JSON-Datei.")
        return
    
    data_dict = {}
    # Thread-Pool für paralleles Abrufen der Daten
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {}
        for airline in data:
            airline_id = airline.get('id')
            url = f"https://flylat.net/company/get_data.php?id={airline_id}"
            futures[executor.submit(scrape_data, url)] = airline_id

        # Ergebnisse aus den Threads verarbeiten
        for future in futures:
            airline_id = futures[future]
            try:
                new_data = future.result()
                if new_data:
                    data_dict[airline_id] = {'data': new_data, 'time': utc_time}
            except Exception as e:
                logging.error("Fehler bei der Verarbeitung der Daten für %s: %s", airline_id, e)

    # Gesammelte Daten speichern
    save_data(data_dict)

if __name__ == "__main__":
    main()