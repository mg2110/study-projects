#!/usr/bin/python3
# -*- coding: latin-1 -*-

from datetime import timedelta, date
import configparser
import datetime
import logging
import sqlite3
import os.path
import requests
import csv
import os
from pathlib import Path

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def read_application_properties(file, section, key):
    # Application Properties ((application.ini) auslesen
    # Quelle: https://zetcode.com/python/configparser/
    #import configparser

    config = configparser.ConfigParser()
    config.read(file)
    value = config[section][key]

    return value


def update_properties_status_values(file, section, system, value):
    # Application properties (application.ini) Werte updaten
    # Quelle: https: // zetcode.com / python / configparser /
    config = configparser.ConfigParser()
    config.read(file)
    cfgfile = open(file, 'w')
    config.set(section, system, value)
    config.write(cfgfile)
    cfgfile.close()


def table_exists(tablename):
    # Prüfung, ob Tabelle existieren
    # Quelle: https://pythonexamples.org/python-sqlite3-check-if-table-exists/
    # Quelle: https://www.geeksforgeeks.org/check-if-table-exists-in-sqlite-using-python/

    # Datenbankname aus Application Properties lesen
    db_name = read_application_properties(file, "database","db_source")

    try:
        c = sqlite3.connect(db_name)  # mit DB verbinden

        c.cursor()  # Ein Curor Objekt mit der cursor() Methode
        # Zähler für die Tabelle mit dem Namen (tablename)
        listOfTables = c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=" + "?",
                                 (tablename,)).fetchall()

        # if das Ergebnis=1, dann existiert die Tabelle
        if listOfTables == [(0,)]:
            logging.info(str(date.today()) + '- TABELLE " + table_name + " ist nicht vorhanden.')
            result = "not"
        else:
            logging.info(str(date.today()) + '- TABELLE " + table_name + " ist vorhanden.')
            result = "exists"

        c.commit()  #
        # DB Verbindung schließen
        c.close()
    except ValueError:
        logging.error(str(date.today()) + '- Fehler mit der Verbindung zur Datenbank')
        result = "Fehler mit der Verbindung zur Datenbank"

    return result


def create_table():
    # Tabelle prices erstellen, wenn sie nicht existiert
    # Quelle: https://www.sqlitetutorial.net/sqlite-python/create-tables/

    db_name = read_application_properties(file, "database",
                                          "db_source")  # Datenbankname aus Application Properties lesen

    connection = sqlite3.connect(db_name)  # DB Verbindung
    cursor = connection.cursor()  ## Ein Curor Objekt mit der cursor() Methode
    # SQL Statement, um die Tabelle prices zu erstellen
    sql_create_table = """ CREATE TABLE IF NOT EXISTS prices (
                                        date         text,                                      
                                        station_uuid text,
                                        diesel       real,
                                        e5           real,
                                        e10          real,
                                        dieselchange integer,
                                        e5change     integer,
                                        e10change    integer
                                    ); """
    # Erstelle Tabelle
    if cursor is not None:
        cursor.execute(sql_create_table)
        logging.info(str(date.today()) + '- TABELLE prices wird erstellt.')
    else:
        logging.error(str(date.today()) + '- Die DB Verbindung kann nicht aufgebaut werden.')

    sql = (
        "create index idx_date on prices (station_uuid, date)")  # Index erstellen, damit SQL Abfragen schneller gehen
    cursor.execute(sql)  # Ausführung SQL Statement
    connection.commit()  # Bestätigung des SQL Statements
    connection.close()  # DB verbindung Schließen


def download_file(url, output_file, compressed=True):
    # Download Historische Datei
    # Quelle: #https://stackoverflow.com/questions/33204944/download-csv-file-using-python-3

    # Hinweis: der stream=True Parameter. Es aktiviert einen optimierten Speicher Support für die Daten
    # beim Laden der Daten
    headers = {}
    if compressed:
        headers["Accept-Encoding"] = "gzip"

    r = requests.get(url, headers=headers, stream=True)

    with open(output_file, 'wb') as f:  # Öfnen mit Block Writes
        for chunk in r.iter_content(chunk_size=4096):
            if chunk:  # Ausfiltern von keep-alive new chunks
                f.write(chunk)
        f.flush()  # Danach zwinge alle Daten in eine Datei (optional)

    return output_file


def import_prices(save_file):
    # Datei prices in DB importieren
    # Quelle: https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python

    #prices = []  # Liste für die Daten aus der heruntergeladenen Datei

    db_name = read_application_properties(file, "database",
                                          "db_source")  # Datenbankname aus Application Properties lesen

    with sqlite3.connect(db_name) as db:  # mit DB verbinden
        cursor = db.cursor()  # DB Cursor Objekt erstellen, um SQL auszuführen
        # Import csv and extract data
        with open(save_file, 'r') as fin:  # Heruntergeladene Datei öffnen
            dr = csv.DictReader(fin)  # Daten einlesen
            prices = [
                (i['date'], i['station_uuid'], i['diesel'], i['e5'], i['e10'], i['dieselchange'], i['e5change'],
                 i['e10change']) for i in dr]

    # SQL Statement, um die Daten in die Tabelle prices einzulesen
    insert_records = "INSERT INTO prices (date, station_uuid, diesel,e5,e10,dieselchange,e5change,e10change) VALUES(?, ?,?,?,?, ?,?,?)"

    # SQL Statement ausführen
    cursor.executemany(insert_records, prices)
    # Bestätigung der Änderungen
    db.commit()
    # Schließen der DB Verbindung
    db.close()
    logging.error(str(date.today()) + ' - CONNECTION DB - für Daten Preise war nicht erfolgreich ')


def import_station(file_stations):
    import pandas as pd
    import sqlite3 as sql

    # Einlesen Tankstellen Datei in Liste
    stations = pd.read_csv(file_stations)
    # Datenbankname aus Application Properties lesen
    db_name = read_application_properties(file, "database", "db_source")

    try:
        # Verbindung DB
        conn = sql.connect(db_name)
        # Tankstellen Datei in DB importieren
        # wenn schon vorhanden, dann die Tabelle ersetzen. Immer neue Tabelle erstellen für
        # die Tankstellen. Ein Update oder Insert auf eine bestehende Tabelle würde
        # zu lange dauern.
        # Eintrag in Logdatei
        logging.info(str(date.today()) + ' - CONNECTION DB - für Daten Stationen importieren war erfolgreich ')
        try:
            stations.to_sql("stations", conn, if_exists="replace")
            # Create your connection.
            cnx = sql.connect(':memory:')
            stations.to_sql(name='stations2', con=cnx)
            p2 = pd.read_sql('select * from stations2', cnx)
            p2.set_index('uuid')  # Index in der Tabelle setzen, damit Abfragen schneller gehen.
            logging.info(str(date.today()) + ' - SQL QUERY - für Daten Stationen importieren war erfolgreich ')
        except:
            logging.error(str(date.today()) + ' - SQL QUERY - für Daten Stationen importieren war nicht erfolgreich ')
        finally:
            conn.commit()
            conn.close()
            logging.info(
                str(date.today()) + ' - CONNECTION DB - für Daten Stationen importieren wurde erfolgreich geschlossen ')
    except:
        logging.error(str(date.today()) + ' - CONNECTION DB - für Daten Stationen importieren war nicht erfolgreich ')


def download_import(c_date):
    global file
    # Funktion für Download und Import der historischen Daten

    # URL für den Download der historischen Dateien zusammensetzen
    url_part2 = c_date[0:4] + "/"
    url_part3 = c_date[5:7] + "/"
    url_date = c_date
    # url_date = str(c_date.strftime("%Y-%m-%d"))
    # url_date = str(datetime.datetime.strptime(c_date,"%Y-%m-%d")) + "/"
    # url_date = str(c_date)
    url_fileextension = "-prices.csv"
    url = url_part1 + url_part2 + url_part3 + url_date + url_fileextension + url_part4
    p_ord=read_application_properties(file, "ordner", "ordner_prices")
    #save_file = "history_files/prices/" + c_date + url_fileextension  # Speicherort für die historischen Daten
    save_file=p_ord + "/" + c_date + url_fileextension
    print(save_file)
    print(url)
    # Prüfung, ob Datei schon existiert
    # Quelle: https://linuxize.com/post/python-check-if-file-exists/
    if os.path.isfile(save_file):
        # Datei existiert schon
        logging.info(
            str(date.today()) + ' - DONWLOAD - Download Datei schon vorhanden ' + str(start_date))  # Eintrag Logdatei
    else:
        # Datei existiert nicht und muss runtergeladen werden
        save_file = download_file(url, save_file, True)
        logging.info(str(date.today()) + ' - DOWNLOAD - Download Datei noch nicht vorhanden ' + str(
            start_date))  # Eintrag Logdatei

    # Import runtergeladene Datei in DB
    import_prices(save_file)

    # Dateiname für historischen Tankstellen
    file_stations = save_file.replace("prices", "stations")
    # URL für Download für historischen Tankstellen
    url_stations = url.replace("prices", "stations")
    if os.path.isfile(file_stations):
        # Datei existiert schon
        logging.info(str(date.today()) + ' - DONWLOAD - Download Stations Datei schon vorhanden ' + str(
            start_date))  # Eintrag Logdatei
    else:
        # Datei existiert nicht und muss runtergeladen werden
        # Download Dateien Tankstellen
        file_stations = download_file(url_stations, file_stations, True)
        logging.info(str(date.today()) + ' - DOWNLOAD - Download Stations Datei noch nicht vorhanden ' + str(
            start_date))  # Eintrag Logdatei

    print(file_path + file_stations)
    # Datei für die Tankstellen in DB importieren
    import_station(file_stations)


    return url_date


def delete_data(delete_date):
    # Housekeeping --> Daten in DB löschen
    # Quelle: https://techoverflow.net/2019/10/14/how-to-fix-sqlite3-python-incorrect-number-of-bindings-supplied-the-current-statement-uses-1-supplied/
    # Quelle: https://www.webucator.com/article/finally-a-use-case-for-finally-python-exception-ha/

    # DB Name aus application.ini
    db_name = read_application_properties(file, "database", "db_source")

    try:
        # Verbindung DB
        conn = sqlite3.connect(db_name)
        # Eintrag in Logdatei
        logging.info(
            str(date.today()) + ' - CONNECTION DB - für Daten löschen (prices) war erfolgreich ' + str(delete_date))
        try:
            cur = conn.cursor()
            cur.execute("delete from prices where date like ?", ['%' + delete_date + '%'])
            logging.info(
                str(date.today()) + ' - SQL QUERY - für Daten löschen (prices) war erfolgreich ' + str(delete_date))
        except:
            logging.error(str(date.today()) + ' - SQL QUERY - für Daten löschen (prices) war nicht erfolgreich ' + str(
                delete_date))
        finally:
            conn.commit()
            conn.close()
            logging.info(
                str(date.today()) + ' - CONNECTION DB - für Daten löschen (prices) wurde erfolgreich geschlossen ' + str(
                    delete_date))
            return "1"
    except:
        logging.error(str(date.today()) + ' - CONNECTION DB - für Daten löschen (prices) war nicht erfolgreich ' + str(
            delete_date))
        return "0"


def drop_table(dtable_name):
    # Tabelle löschen, wenn man die DB neu aufsetzen möchte

    # DB Name aus application.ini
    db_name = read_application_properties(file, "database", "db_source")
    try:
        # Verbindung DB
        connection = sqlite3.connect(db_name)
        logging.info(str(datetime.date.today().strftime(
            '%Y-%m-%d %H:%M')) + ' - DB Verbindung für Tabelle ' + dtable_name + ' war erfolgreich ')
        try:
            # Cursor Objekt erstellen
            cursor = connection.cursor()
            # SQL Statement für das Löschen der Tabelle
            droptablestatement = "DROP TABLE " + dtable_name
            # SQL Statement ausführen
            cursor.execute(droptablestatement)
            logging.info(str(datetime.today().strftime(
                '%Y-%m-%d %H:%M')) + ' - SQL QUERY - für Tabelle ' + dtable_name + ' löschen war erfolgreich ')
        except:
            logging.error(str(datetime.date.today().strftime('%Y-%m-%d %H:%M')) + ' - SQL QUERY - ' + dtable_name + ' war nicht erfolgreich ')

        finally:
            connection.commit()
            # Verbindung schließen
            connection.close()
            logging.info(str(datetime.date.today().strftime(
                '%Y-%m-%d %H:%M')) + ' - CONNECTION DB - für ' + dtable_name + ' wurde erfolgreich geschlossen ')

    except:
        logging.error(str(datetime.date.today().strftime(
            '%Y-%m-%d %H:%M')) + ' - CONNECTION DB - für ' + dtable_name + '  war nicht erfolgreich ')





##################### START ################################
# Application Properties file --> um Werte zu ändern ohne den Sourcecode ändern zu müssen
file = "application.ini"

# Ordner für die Log Dateien
section = "ordner"
key = "ordner_logs"
# Ordnername aus application.ini auslesen
ord_logs = read_application_properties(file, section, key)

# Prüfen, ob Ordner prices extsiert, wenn nicht erstellen
if not os.path.exists(ord_logs):
    os.makedirs(ord_logs)

# Logging, um in einer log Datei Fehler, Stati usw. lesen zu können.
# Quelle: https://docs.python.org/3/howto/logging.html
log_date = datetime.date.today()  # Log Datum --> Name der Logdatei
log_filename = "import_" + str(log_date) + ".log"
logging.basicConfig(filename='logs/' + log_filename, encoding='utf-8', level=logging.DEBUG)
logging.info(str(date.today()) + '  - START APP Benzinpreis.')

# Prüfung, ob application.ini existiert
# if os.path.exists(file):
if not os.path.isfile(file):
    logging.error(str(date.today()) + ' - APPLICATION.INI - Die Datei application.ini existiert nicht !!!')
    exit()

# Prüfung wie das Skript ausgeführt werden soll und ob es
# ausgeführt werden soll
section = "start"
key = "start_option"
start_option = read_application_properties(file, section, key)
if start_option == "2":
    print ("ENDE Import")
    exit()

# Pfad für die historischen Daten
file_path = os.path.dirname(os.path.abspath(__file__)) + "\/"

# Prüfung, ob Ordner existieren, falls nicht erstellen
# Quelle: https://www.delftstack.com/de/howto/python/python-create-directory/
# Ordner für die historischen Benzinpreise
section = "ordner"
key = "ordner_prices"
# Ordnername aus application.ini auslesen
ord_prices = read_application_properties(file, section, key)

# Prüfen, ob Ordner prices extsiert, wenn nicht erstellen
if not os.path.exists(ord_prices):
    os.makedirs(ord_prices)

# Ordner für die Tankstellen(Stations)
section = "ordner"
key = "ordner_stations"
# Ordnername aus application.ini auslesen
ord_stations = read_application_properties(file, section, key)

# Prüfen, ob Ordner prices extsiert, wenn nicht erstellen
if not os.path.exists(ord_stations):
    os.makedirs(ord_stations)

## Application.ini --> Sektion [download] --> download
# Bedeutung der Werte:
# 0 --> Initial Download (das erste Mal historischen Daten einlesen)
# 1 --> Update der historischen Daten bis zum aktuellen Datum - 1 Tag + Housekeeping (optional)
# 2 --> Datenbank komprimieren
# 3 --> Tabellen "prices" und "stations" löschen
# 4 --> Housekeeping
section = "download"
key = "download"
download = read_application_properties(file, section, key)

# Wert in der application.ini legt fest, ob die Download Dateien gelöscht
# werden oder nicht
dateien_loeschen = read_application_properties(file, section, "dateien_loeschen")
print(dateien_loeschen)
# Dateien löschen, wenn Wert=1 in application.ini
# Quelle: https://linuxize.com/post/python-delete-files-and-directories/
if int(dateien_loeschen) == 1:
    pfile=os.path.dirname(os.path.abspath(__file__)) + "/\/" + ord_prices + "/"
    for f in Path(pfile).glob('*.csv'):
        try:
            f.unlink()
        except OSError as e:
            print("Error: %s : %s" % (f, e.strerror))

    sfile = os.path.dirname(os.path.abspath(__file__)) + "/\/" + ord_stations + "/"
    for d in Path(sfile).glob('*.csv'):
        try:
            d.unlink()
        except OSError as e:
            print("Error: %s : %s" % (d, e.strerror))

# Startdatum für den Download der historischen Daten
start_year = read_application_properties(file, section, "start_year")  # Startjahr für den download
start_month = read_application_properties(file, section, "start_month")  # Startmonat für den download

# Startdatum für den Download
start_date = date(int(start_year), int(start_month), 1)  # Startdatum

# Enddatum für den Download der historischen Daten (aktuelles Tagesdatum - 1 Tag)
# Quelle: https://www.delftstack.com/de/howto/python/python-current-year/
end_date = datetime.date.today()
tbl_result = ""

# URL für Download aus application.ini
url_part1 = read_application_properties(file, section, "url_prices1")
url_part4 = read_application_properties(file, section, "url_prices2")
url_part4 = url_part4.replace("[p]", "%")
url_part4 = url_part4.replace("[u]", "&")

tbl_result = table_exists("prices")  # Prüfung, ob die Tabelle prices existiert
logging.info(str(date.today()) + '- Start Prüfung, ob die Tabelle prices existiert: ' + str(start_date))

if tbl_result == "not":
    logging.info(str(date.today()) + '- TABELLE prices existiert nicht. Sie wird erstellt: ' + str(start_date))
    create_table()  # Tabelle erstellen

if int(download) == 0:
    u_date=""
    # https://www.it-swarm.com.de/de/python/durchlaufen-einer-reihe-von-datumsangaben-python/967223095/
    # Schleife für Download der historischen Daten (aktuelles Datum - 1 Tag)
    for single_date in daterange(start_date, end_date):
        current_date = single_date.strftime("%Y-%m-%d")
        # print(current_date)
        # current_date = datetime.datetime.strptime(single_date, '%Y-%m-%d').date()
        u_date = download_import(current_date)

    # Letztes Download Datum in application.ini eintragen, damit beim nächsten Aufruf nur
    # noch Update gemacht wird
    update_properties_status_values("application.ini", "download", "end_date", u_date)  # letztes Datum
    update_properties_status_values("application.ini", "download", "download",
                                    "1")  # 1 --> bedeutet dann nur noch Update

# Update, es werden nur die historischen Daten bis zum aktuellen Datum - 1 Tag verarbeitet
elif int(download) == 1:
    # Wenn die Startdatum aus der application.ini gleich dem (aktuellen Datum -1 Tag) ist,
    # dann muss auch keine Datei herunterlgeladen werden. Import ist auf aktuellen Stand
    url_date = ""

    # Enddatum für den Download der historischen Daten (aktuelles Tagesdatum - 1 Tag)
    # Quelle: https://www.delftstack.com/de/howto/python/python-current-year/
    end_date = datetime.date.today()  # End Datum für den Download
    start_date = read_application_properties(file, section, "end_date")  # Startdatum + 1 Tag für den download

    # Quelle: https://appdividend.com/2020/10/27/how-to-convert-python-string-to-date/
    # Datum aus application.ini von String in Date konvertieren
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    # Startdatum - 1 Tag, weil er sonst die Datei vom letzten Import holen würde
    start_date = start_date + timedelta(days=1)
    u_date = ""  # Letztes Datum für das Update wird in application.ini eingetragen
    # damit die App weiß, ab welchem Datum das Update durgeführt werden soll
    # https://www.it-swarm.com.de/de/python/durchlaufen-einer-reihe-von-datumsangaben-python/967223095/
    # Schleife für Download der historischen Daten (letztes Datum + 1 Tag bis zum aktuellen Datum-1)
    for single_date in daterange(start_date, end_date):  # Schleife für das Update
        #print(single_date.strftime("%Y-%m-%d"))
        current_date = single_date.strftime("%Y-%m-%d")  # erstes Datum für das Update
        u_date = download_import(current_date)  # Funktion für Download der Daten und Import in DB

        # Letztes Download Datum in application.ini eintragen, damit beim nächsten Aufruf nur
        # noch die Updates gemacht werden, die noch nicht gemacht wurden.
        if len(u_date) > 0:
            update_properties_status_values("application.ini", "download", "end_date", u_date)

        # Housekeeping --> Daten löschen, damit die DB nicht zu groß wird
        housekeeping = read_application_properties(file, section, "housekeeping")  # Wert 0 --> kein Housekeeping
        # Wert 1 --> Housekeeping
        if int(housekeeping) == 1:
            # Zeitraum in Tagen für Daten, die in der DB vorhanden sein sollen
            period = read_application_properties(file, section, "daten_zeitraum")
            # Berechnung, ob Daten vorhanden sind, die gelöscht werden können
            # Quelle: https://www.kite.com/python/answers/how-to-subtract-days-from-a-date-in-python
            days = datetime.timedelta(int(period))
            del_date = single_date - days
            print (del_date)
            result = delete_data(str(del_date))
            logging.info(str(date.today()) + ' - HOUSEKEEPING - Daten vom " + str(del_date) + " wurden gelöscht ')

elif int(download) == 2:
    # DB komprimieren, falls man die DB kopieren möchte, um
    # sie auf einem anderen PC zu verwenden

    # DB Name aus application.ini
    db_name = read_application_properties(file, "database", "db_source")
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    db_path = os.path.join(BASE_DIR, db_name)
    # Verbinden mit DB
    conn = sqlite3.connect(db_path)
    # Befehl für DB Komprimierung
    conn.execute("VACUUM")
    # DB schließen
    conn.close()
elif int(download) == 3:
    # Tabellen löschen
    drop_table("prices")
    drop_table("stations")
elif int(download) == 4:
    # Housekeeping ohne Import
    # Datum bis wann die Daten Prices in der DB gelöscht werden sollen
    end_date = read_application_properties(file, section, "delete_date_bis")  # Startdatum + 1 Tag für den download
    # Datum ab wann die Daten aus DB Prices gelöscht werden sollen
    start_date = read_application_properties(file, section, "delete_date_von")  # Startdatum + 1 Tag für den download
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()

    for single_date in daterange(start_date, end_date):  # Schleife für das Update
        del_date = single_date
        # Funktion für Daten Preise löschen
        result = delete_data(str(del_date))
        logging.info(str(date.today()) + ' - HOUSEKEEPING - Daten vom " + str(del_date) + " wurden gelöscht ')



print("Fertig")