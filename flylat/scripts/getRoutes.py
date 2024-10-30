import time
import json
import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from dotenv import load_dotenv

def mkdir(folder):
    """Erstellt einen Ordner, falls dieser nicht existiert."""
    if not os.path.exists(folder):
        os.makedirs(folder)

class Airport:
    """Repräsentiert einen Flughafen mit ICAO-Code, Name, Latitude und Longitude."""

    def __init__(self, icao, name, latitude, longitude, iso_country, municipality, iso_region):
        self.icao = icao
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
        self.iso_country = iso_country
        self.municipality = municipality
        self.iso_region = iso_region

class Hashtable:
    """Hash-Tabelle mit einfachem Chaining zur Behandlung von Kollisionen."""

    def __init__(self):
        self.size = 78007
        self.table = [[] for _ in range(self.size)]

    def hashfunction(self, key):
        """Erstellt einen Hashwert für einen gegebenen ICAO-Key."""
        hash_value = 0
        prime_number = 79
        for char in key:
            hash_value = hash_value * prime_number + ord(char)
        return hash_value % self.size

    def addAirport(self, airport):
        """Fügt einen Flughafen zur Hash-Tabelle hinzu."""
        index = self.hashfunction(airport.icao)
        for entry in self.table[index]:
            if entry.icao == airport.icao:
                return
        self.table[index].append(airport)

    def saveTable(self, fileName):
        """Speichert die Hash-Tabelle als JSON-Datei."""
        data = []
        export_folder = "flylat/data/"
        mkdir(export_folder)
        file_path = os.path.join(export_folder, f"{fileName}.json")

        for index, airports in enumerate(self.table):
            if airports:
                for airport in airports:
                    airport_data = {
                        "Index": index,
                        "ICAO": airport.icao,
                        "Name": airport.name,
                        "Latitude": airport.latitude,
                        "Longitude": airport.longitude,
                        "Municipality": airport.municipality,
                        "iso_country": airport.iso_country,
                        "Iso_Region": airport.iso_region
                    }
                    data.append(airport_data)

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file)
        print(f"Hashtable-Daten wurden unter {file_path} gespeichert")

    def loadTable(self, fileName):
        """Lädt eine Hash-Tabelle aus einer JSON-Datei."""
        import_folder = "flylat/data/"
        file_path = os.path.join(import_folder, f"{fileName}.json")
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
                for item in data:
                    airport = Airport(
                        item["ICAO"], item["Name"], item["Latitude"], item["Longitude"], 
                        item["iso_country"], item["Municipality"], item["Iso_Region"]
                    )
                    self.addAirport(airport)
            print(f"Hash-Tabelle aus {file_path} geladen")
        except FileNotFoundError:
            print(f"Datei {file_path} wurde nicht gefunden.")
        except json.JSONDecodeError:
            print(f"Fehler beim Dekodieren der JSON-Datei: {file_path}")
        except Exception as e:
            print(f"Fehler beim Laden der Datei: {e}")

def get_airport_info(icao_code):
    """Holt Informationen zu einem Flughafen über eine externe API."""
    load_dotenv()
    api_token = os.getenv("API_TOKEN")
    url = f"https://airportdb.io/api/v1/airport/{icao_code}?apiToken={api_token}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return (
            data.get("name"), data.get("latitude_deg"), data.get("longitude_deg"),
            data.get("iso_country"), data.get("municipality"), data.get("iso_region")
        )
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen der Daten für {icao_code}: {e}")
        return None, None, None, None, None, None

def get_routes(path, table):
    """Verarbeitet Routen und bereichert die Flughafendaten."""
    apiCount = 0
    with open(path, "r") as f:
        data = json.load(f)
        routes = data["routes"]
        newdata = {
            "name": data["name"],
            "id": data["id"],
            "updateTimestamp": time.time(),
            "routes": [],
        }

        for item in tqdm(routes):
            enriched_route = {
                "route_id": item.get("route_id"),
                "profit": item.get("profit"),
                "ticketpp": item.get("ticketpp"),
                "distance": item.get("distance"),
                "flown": item.get("flown"),
                "verified": item.get("verified"),
                "departure": None,
                "destination": None,
            }
            # Departure und Destination anreichern
            for location_key in ["departure", "destination"]:
                location_info = item.get(location_key)
                if isinstance(location_info, dict):
                    icao_code = location_info.get("ICAO")
                    index = table.hashfunction(icao_code)
                    airport_info = None
                    for airport in table.table[index]:
                        if airport.icao == icao_code:
                            airport_info = (
                                airport.name, airport.latitude, airport.longitude,
                                airport.iso_country, airport.municipality, airport.iso_region
                            )
                            break

                    if not airport_info:
                        airport_info = get_airport_info(icao_code)
                        apiCount += 1
                        if all(airport_info):
                            table.addAirport(Airport(icao_code, *airport_info))
                    enriched_route[location_key] = {
                        "ICAO": icao_code,
                        "name": airport_info[0] if airport_info else None,
                        "latitude": airport_info[1] if airport_info else None,
                        "longitude": airport_info[2] if airport_info else None,
                        "municipality": airport_info[4] if airport_info else None,
                        "iso_country": airport_info[3] if airport_info else None,
                        "iso_region": airport_info[5] if airport_info else None
                    }
            newdata["routes"].append(enriched_route)
        folder = "flylat/data/routes/"
        os.makedirs(folder, exist_ok=True)
        newpath = os.path.join(folder, f"{data['id']}.json")
        with open(newpath, "w", encoding="utf-8") as f:
            json.dump(newdata, f, ensure_ascii=False, indent=4)
        print(f"Routen angereichert und gespeichert. API-Aufrufe: {apiCount}")
        
def extract_departure_destination(id):
    """Extracts route data from a web service and saves it temporarily."""
    url = f"https://flylat.net/company/get_routes.php?id={id}"
    name_url = f"https://flylat.net/company/{id}"

    try:
        name_response = requests.get(name_url)
        name_response.raise_for_status()
        soup = BeautifulSoup(name_response.text, "html.parser")
        airline_name_tag = soup.find("td", string="Airline Name").find_next_sibling("td")
        airline_name = airline_name_tag.text.strip() if airline_name_tag else None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching airline name for ID {id}: {e}")
        return None
    except Exception as e:
        print(f"Error processing airline name for ID {id}: {e}")
        return None

    data = {
        "name": airline_name,
        "id": id,
        "routes": [],
    }

    try:
        response = requests.get(url)
        response.raise_for_status()
        routes = response.json()
        
        # Jede Route erhält eine fortlaufende ID
        for idx, route in enumerate(tqdm(routes)):
            # Aktualisierung der Struktur für departure und destination
            departure = {"ICAO": route["dep"]}
            destination = {"ICAO": route["des"]}
            
            data["routes"].append({
                "route_id": idx,
                "departure": departure,
                "destination": destination,
                "profit": route["profit"],
                "ticketpp": route["ticketpp"],
                "distance": route["distance"],
                "flown": route["flown"],
                "verified": route["verified"]
            })
    except requests.exceptions.RequestException as e:
        print(f"Error fetching routes for airline ID {id}: {e}")
        return None
    except Exception as e:
        print(f"Error processing routes for airline ID {id}: {e}")
        return None

    save_path = f"flylat/data/tmp_{id}.json"
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        tqdm.write("Extracted data saved successfully.")
    return save_path

def main():
    """Main function that loads airports, processes routes, and updates the table."""
    
    print("Starting route data retrieval...")
    filename = "airports"
    airline_file = "flylat/data/airlines.json"
    airport_table = Hashtable()
    airport_table.loadTable(filename)
    with open(airline_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    for airline in data["airlines"]:
        airline_id = airline["id"]
        path = extract_departure_destination(airline_id)
        if path:
            get_routes(path, airport_table)
            if os.path.exists(path):
                os.remove(path)
            print(f"Data has been retrieved for airline ID: {airline_id}")
        else:
            print(f"Data could not be retrieved for airline ID: {airline_id}")
    airport_table.saveTable("airports")

if __name__ == "__main__":
    main()
