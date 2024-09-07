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
    """Represents an airport with ICAO code, name, latitude, and longitude."""

    def __init__(self, icao, name, latitude, longitude):
        self.icao = icao
        self.name = name
        self.latitude = latitude
        self.longitude = longitude


class Hashtable:
    """Hashtable implementation with simple chaining to handle collisions."""

    def __init__(self):
        self.size = 78007  # 60000 Airports * 30% + 7 for prime number
        self.table = [[] for _ in range(self.size)]  # Use list for chaining

    def hashfunction(self, key):
        """Generates a hash value for a given ICAO key."""
        hash_value = 0
        prime_number = 79
        for char in key:
            hash_value = hash_value * prime_number + ord(char)
        return hash_value % self.size

    def addAirport(self, airport):
        """Adds an airport to the hash table, resolving collisions using chaining."""
        index = self.hashfunction(airport.icao)
        for entry in self.table[index]:
            if entry.icao == airport.icao:
                print(f"Airport {airport.icao} already exists in the table.")
                return
        self.table[index].append(airport)

    def saveTable(self, fileName):
        """Saves the hash table as a JSON file."""
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
                    }
                    data.append(airport_data)

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file)

        print(f"Hashtable data saved to {file_path}")

    def loadTable(self, fileName):
        """Loads a hash table from a JSON file."""
        import_folder = "flylat/data/"
        mkdir(import_folder)

        file_path = os.path.join(import_folder, f"{fileName}.json")

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
                for item in data:
                    airport = Airport(
                        item["ICAO"], item["Name"], item["Latitude"], item["Longitude"]
                    )
                    self.addAirport(airport)
                print(f"Hashtable data loaded from {file_path}")
        except FileNotFoundError:
            print(f"The specified file was not found: {file_path}")
        except json.JSONDecodeError:
            print(f"Error decoding the JSON file: {file_path}")
        except Exception as e:
            print(f"An error occurred while loading the file: {e}")
            return None


def get_airport_info(icao_code):
    """Fetches airport information using an external API."""
    load_dotenv()

    api_token = os.getenv("API_TOKEN")
    url = f"https://airportdb.io/api/v1/airport/{icao_code}?apiToken={api_token}"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises exception for HTTP errors
        data = response.json()

        name = data.get("name")
        latitude = data.get("latitude_deg")
        longitude = data.get("longitude_deg")

        return name, latitude, longitude
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {icao_code}: {e}")
        return None, None, None


def get_routes(path, table):
    """Processes routes and enriches the airport data."""
    with open(path, "r") as f:
        data = json.load(f)

        routes = data["routes"]
        newdata = {
            "name": data["name"],
            "id": data["id"],
            "updateTimestamp": time.time(),
            "routes": [],
        }

        count_missing_airports = 0

        for item in tqdm(routes):
            for airport_type in ["departure", "destination"]:
                if airport_type in item:
                    icao_code = item[airport_type]
                    index = table.hashfunction(icao_code)
                    airport_info = None

                    for airport in table.table[index]:
                        if airport.icao == icao_code:
                            airport_info = (
                                airport.name,
                                airport.latitude,
                                airport.longitude,
                            )
                            break

                    if not airport_info:
                        airport_info = get_airport_info(icao_code)
                        name, latitude, longitude = airport_info
                        if name and latitude and longitude:
                            table.addAirport(
                                Airport(icao_code, name, latitude, longitude)
                            )
                        else:
                            count_missing_airports += 1

                    item[airport_type] = {
                        "ICAO": icao_code,
                        "name": airport_info[0] if airport_info else None,
                        "latitude": airport_info[1] if airport_info else None,
                        "longitude": airport_info[2] if airport_info else None,
                    }
        newdata["routes"] = routes
        folder = "flylat/data/routes/"
        mkdir(folder)
        newpath = os.path.join(folder, f"{data['id']}.json")
        with open(newpath, "w") as f:
            json.dump(newdata, f)
        return count_missing_airports


def extract_departure_destination(id):
    """Extracts route data from a web service and saves it temporarily."""
    url = f"https://flylat.net/company/get_routes.php?id={id}"
    name_url = f"https://flylat.net/company/{id}"

    try:
        name_response = requests.get(name_url)
        name_response.raise_for_status()
        soup = BeautifulSoup(name_response.text, "html.parser")
        airline_name_tag = soup.find("td", text="Airline Name").find_next_sibling("td")
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
        for route in tqdm(routes):
            departure = route["dep"]
            destination = route["des"]
            data["routes"].append({"departure": departure, "destination": destination})
    except requests.exceptions.RequestException as e:
        print(f"Error fetching routes for airline ID {id}: {e}")
        return None
    except Exception as e:
        print(f"Error processing routes for airline ID {id}: {e}")
        return None
    save_path = f"flylat/data/tmp_{id}.json"
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
        tqdm.write("Extracted data saved successfully.")
    return save_path


def main():
    """Main function that loads airports, processes routes, and updates the table."""
    filename = "airports"
    airline_file = "flylat/data/airlines.json"
    airport_table = Hashtable()
    airport_table.loadTable(filename)
    count_missing_airports = 0
    with open(airline_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    for airline in data["airlines"]:
        airline_id = airline["id"]
        path = extract_departure_destination(airline_id)
        if path:
            count_missing_airports += get_routes(path, airport_table)
            if os.path.exists(path):
                os.remove(path)
            print(f"Data has been retrieved for airline ID: {airline_id}")
        else:
            print(f"Data could not be retrieved for airline ID: {airline_id}")
    airport_table.saveTable("airports")
    print(f"Missing airports: {count_missing_airports}")


if __name__ == "__main__":
    main()
