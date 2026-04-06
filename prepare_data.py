import csv
import os
import logging
import pip._vendor.requests as requests
from datetime import datetime
from collections import defaultdict
from pathlib import Path


TODAY_STR = datetime.today().strftime('%Y_%m_%d')
URL = "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland/raw/refs/heads/main/Aktuell_Deutschland_SarsCov2_Infektionen.csv?download="
INPUT_FILE = "Aktuell_Deutschland_SarsCov2_Infektionen_" + TODAY_STR + ".csv"
OUTPUT_FILE = "Bereinigte_Daten_" + TODAY_STR + ".csv"
LOG_FILE = "fehler.log"

# Logging einrichten
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def download_file(url, filename):
    try:
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()  # Raises HTTPError for bad responses

            with open(filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive chunks
                        file.write(chunk)

        print(f"Download erfolgreich: {filename}")

    except Exception as e:
        logging.error(f"Fehler beim Herunterladen der Datei: {e}")
        # Delete partially downloaded file if it exists
        if os.path.exists(filename):
            try:
                os.remove(filename)
                logging.debug(f"Unvollständige Datei gelöscht: {filename}")
            except Exception as delete_error:
                logging.error(f"Fehler beim Löschen der Datei: {delete_error}")
        raise


def process_csv(input_file, output_file):
    try:
        with open(input_file, newline='', encoding='utf-8') as csvfile, \
             open(output_file, 'w', newline='', encoding='utf-8') as out_csv:

            reader = csv.reader(csvfile, delimiter=',')
            writer = csv.writer(out_csv, delimiter=',')
            
            # Header schreiben
            writer.writerow(["IdLandkreis", "Altersgruppe", "Geschlecht", "Refdatum", 
                             "AnzahlFall", "AnzahlTodesfall", "AnzahlGenesen"])

            next(reader)  # CSV-Header überspringen
            buffer = {}   # Zwischenspeicher für ein Refdatum
            prevRefDate = None

            for row in reader:
                try:
                    cols = [row[i] for i in [0, 1, 2, 4, 9, 10, 11]]
                except IndexError:
                    continue  # Zeile überspringen, falls nicht genug Spalten

                county, ageGroup, gender, date = cols[:4]
                if ageGroup == "unbekannt":
                    ageGroup = "null"
                else:
                    ageGroup = ageGroup.replace("A","")
                if gender == "unbekannt":
                    gender = "null"

                infected, died, recovered = map(lambda x: int(x) if x else 0, cols[4:7])

                key = (county, ageGroup, gender)
                # Nettowerte direkt vorbereiten
                cases = infected - died - recovered
                value = (cases, died, recovered)

                # Datum gewechselt → Puffer direkt schreiben
                if prevRefDate is not None and date != prevRefDate:
                    for k, v in buffer.items():
                        writer.writerow(list(k) + [prevRefDate] + list(v))
                    buffer.clear()

                # Aggregation
                if key in buffer:
                    oldCases, oldDeaths, oldRecovered = buffer[key]
                    buffer[key] = (oldCases + value[0], oldDeaths + value[1], oldRecovered + value[2])
                else:
                    buffer[key] = value

                prevRefDate = date

            # Letzte Pufferwerte schreiben
            for k, v in buffer.items():
                writer.writerow(list(k) + [prevRefDate] + list(v))

        print(f"Daten erfolgreich bereinigt und gespeichert: {output_file}")

    except Exception as e:
        logging.error(f"Fehler bei der Verarbeitung der CSV: {e}")
        # Delete partially converted file if it exists
        if os.path.exists(output_file):
            try:
                os.remove(output_file)
                logging.debug(f"Unvollständige Datei gelöscht: {output_file}")
            except Exception as delete_error:
                logging.error(f"Fehler beim Löschen der Datei: {delete_error}")
        raise

def maxima_pro_geschlecht(csv_file):
    try:
        maxima = defaultdict(lambda: {
            "AnzahlFall": {"Wert": -1, "Zeile": None},
            "AnzahlTodesfall": {"Wert": -1, "Zeile": None},
            "AnzahlGenesen": {"Wert": -1, "Zeile": None},
            "Todesfall_plus_Genesen": {"Wert": -1, "Zeile": None}
        })

        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                geschlecht = row.get("Geschlecht", "unbekannt")
                try:
                    anzahl_fall = int(row.get("AnzahlFall", 0))
                    anzahl_todesfall = int(row.get("AnzahlTodesfall", 0))
                    anzahl_genesen = int(row.get("AnzahlGenesen", 0))
                except ValueError:
                    continue  # Zeile überspringen, falls ungültige Zahlen

                # Maxima prüfen und ggf. ganze Zeile speichern
                if anzahl_fall > maxima[geschlecht]["AnzahlFall"]["Wert"]:
                    maxima[geschlecht]["AnzahlFall"] = {"Wert": anzahl_fall, "Zeile": row}

                if anzahl_todesfall > maxima[geschlecht]["AnzahlTodesfall"]["Wert"]:
                    maxima[geschlecht]["AnzahlTodesfall"] = {"Wert": anzahl_todesfall, "Zeile": row}

                if anzahl_genesen > maxima[geschlecht]["AnzahlGenesen"]["Wert"]:
                    maxima[geschlecht]["AnzahlGenesen"] = {"Wert": anzahl_genesen, "Zeile": row}

                sum_todes_genesen = anzahl_todesfall + anzahl_genesen
                if sum_todes_genesen > maxima[geschlecht]["Todesfall_plus_Genesen"]["Wert"]:
                    maxima[geschlecht]["Todesfall_plus_Genesen"] = {"Wert": sum_todes_genesen, "Zeile": row}

        # Nur die Zeilen zurückgeben
        result = {}
        for geschlecht, data in maxima.items():
            result[geschlecht] = {
                "AnzahlFall": data["AnzahlFall"]["Zeile"],
                "AnzahlTodesfall": data["AnzahlTodesfall"]["Zeile"],
                "AnzahlGenesen": data["AnzahlGenesen"]["Zeile"],
                "Todesfall_plus_Genesen": data["Todesfall_plus_Genesen"]["Zeile"]
            }

        for geschlecht, werte in result.items():
            print(f"\nGeschlecht: {geschlecht}")
            for typ, zeile in werte.items():
                print(f"Max {typ}: {zeile}")
    except Exception as e:
                logging.error(f"Fehler bei der Verarbeitung der CSV: {e}")
                raise

def delete_old_files(prefix: str, directory: str = "."):
    path = Path(directory)
    
    for file in path.iterdir():
        if (
            file.is_file()
            and file.name.startswith(prefix)
            and TODAY_STR not in file.name
        ):
            try:
                file.unlink()
            except Exception as e:
                logging.error(f"Fehler beim Löschen von {file}: {e}")

try:
    if not os.path.exists(INPUT_FILE):
        download_file(URL, INPUT_FILE)
        logging.debug(f"Finished download: {TODAY_STR}")
        delete_old_files("Aktuell_Deutschland_SarsCov2_Infektionen")
    else:
        logging.debug("Skipped download, file is already present")

    if not os.path.exists(OUTPUT_FILE):
        process_csv(INPUT_FILE, OUTPUT_FILE)
        logging.debug(f"Finished conversion: {TODAY_STR}")
        delete_old_files("Bereinigte_Daten")
    else:
        logging.debug("Skipped conversion, file is already present")

    maxima = maxima_pro_geschlecht(OUTPUT_FILE)
    
except Exception as e:
    print("Es ist ein Fehler aufgetreten. Details siehe Log-Datei.")