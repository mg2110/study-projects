import matplotlib.pyplot as plt
import sqlite3 as sql
from datetime import datetime, timedelta
import locale
import numpy as np

# Ändern der Schreibweise für Tage auf Deutsch
locale.setlocale(locale.LC_ALL, "german")

# Application Properties Datei
file = "application.ini"


def createChart_lastDay(ID, today, count):
    # Prüfen, ob Today als String oder Zeitstempel übergeben wurde
    if isinstance(today, str):
        print("STR TO DATETIME")
        today = datetime.strptime(today, '%Y-%m-%d')

    # Ermittlung des letzten Tages im korrekten Format
    strLastDay = today.strftime('%Y-%m-%d')

    # Verbindung zur Datenbank herstellen
    connection = sql.connect('benzin_db.sqlite')
    c = connection.cursor()

    # Auslesen der Daten (50 Datensätze, da die Anzahl der erfassten Daten je Tag unterschiedlich sind)
    c.execute("SELECT date, diesel, e5, e10 FROM prices WHERE station_uuid = (?) ORDER BY date DESC LIMIT 50", (ID,))
    fetchedData = c.fetchall()

    # Verbindung zur Datenbank beenden
    connection.close()

    # Prüfen, ob die Datensätze in der Datenbank mit dem übergebenen aktuellem Datum übereinstimmen
    checkToday = list(fetchedData[0])[0].split(' ')[0]
    if checkToday == strLastDay:
        print("CHECK OK")
        status = 'ok'
    else:
        print("CHECK FAILED!")
        status = 'failed'
        error = "Das angegebene Datum stimmt nicht mit den Datensätzen in der Datenbank überein!"

    if status == 'ok':
        # Bereinigung der Daten
        # Es werden nur Daten des vorherigen Tages eingelesen
        # Es werden doppelte Datensätze herausgefiltert
        # Es wird nur ein Datensatz je Stunde verwendet
        data = []
        dataCheck = []
        for entry in fetchedData:
            temp = entry[0].split(' ')
            if temp[0] == strLastDay:
                timeStamp = entry[0].split(' ')[1].split(':')[0]
                if temp[1] not in dataCheck:
                    if timeStamp not in dataCheck:
                        data.append(entry)
                        dataCheck.append(timeStamp)
                        dataCheck.append(temp[1])

        # Achsen werden mit Daten befüllt für Diesel, E5 und E10
        achseX_temp = []
        achseY_diesel_temp = []
        achseY_e5_temp = []
        achseY_e10_temp = []
        for entry in data:
            # timeStamp: Vorbereitung der X-Achse, sodass nur die Stunde ausgegeben wird
            timeStamp = entry[0].split(' ')[1].split(':')[0]
            achseX_temp.append(timeStamp)
            achseX = list(reversed(achseX_temp))
            achseY_diesel_temp.append(entry[1])
            achseY_diesel = list(reversed(achseY_diesel_temp))
            achseY_e5_temp.append(entry[2])
            achseY_e5 = list(reversed(achseY_e5_temp))
            achseY_e10_temp.append(entry[3])
            achseY_e10 = list(reversed(achseY_e10_temp))

        # Löscht die vorherigen intern gespeicherten Plots
        plt.clf()
        plt.cla()

        # Plotten der Diagramme
        plt.plot(achseX, achseY_diesel, 'ok-', label='Diesel')
        plt.plot(achseX, achseY_e5, 'ob-', label='Benzin E5')
        plt.plot(achseX, achseY_e10, 'og-', label='Benzin E10')
        plt.legend(loc='upper right')
        plt.grid(which='both')
        plt.title('Preisverlauf des letzten Tages')
        plt.xlabel('Uhrzeit')
        plt.ylabel('Preis €/l')
        bildname = 'assets/ChartLastDay' + str(count) + '.png'
        plt.savefig(bildname)


def createChart_last7Days(ID, today, count):
    # Prüfen, ob Today als String oder Zeitstempel übergeben wurde
    if isinstance(today, str):
        print("STR TO DATETIME")
        today = datetime.strptime(today, '%Y-%m-%d')

    # Ermittlung der letzten 7 Tage
    strLast7Days = []
    today = today + timedelta(days=1)
    for i in range(1, 8, 1):
        subDays = today - timedelta(days=i)
        strLast7Days.append(subDays.strftime('%Y-%m-%d'))

    # Verbindung zur Datenbank herstellen
    connection = sql.connect('benzin_db.sqlite')
    c = connection.cursor()

    # Auslesen der Daten (7*50 Datensätze, da die Anzahl der erfassten Daten je Tag unterschiedlich sind)
    c.execute("SELECT date, diesel, e5, e10 FROM prices WHERE station_uuid = (?) ORDER BY date DESC LIMIT 350", (ID,))
    fetchedData = c.fetchall()

    # Verbindung zur Datenbank beenden
    connection.close()

    # Prüfen, ob die Datensätze in der Datenbank mit dem übergebenen aktuellem Datum übereinstimmen
    checkToday = list(fetchedData[0])[0].split(' ')[0]
    if checkToday == strLast7Days[0]:
        print("CHECK OK")
        status = 'ok'
    else:
        print("CHECK FAILED!")
        status = 'failed'
        error = "Das angegebene Datum stimmt nicht mit den Datensätzen in der Datenbank überein!"

    if status == 'ok':
        # Bereinigung der Daten
        # Abgleich, ob der Eintrag zu den letzten 7 Tagen gehört
        # Abgleich, ob für den Tag bereits ein erster Eintrag vorliegt
        # Wenn nicht, wird er angelegt
        # Wenn ja, wird der Preis aufsummiert
        # Zusätzlich wird die Anzahl der Einträge für die einzelnen Tage ermittelt
        datesCounter = {}
        dictDiesel = {}
        dictE5 = {}
        dictE10 = {}
        for entry in fetchedData:
            temp = entry[0].split(' ')
            if temp[0] in strLast7Days:
                if temp[0] not in datesCounter:
                    dictDiesel[temp[0]] = entry[1]
                    dictE5[temp[0]] = entry[2]
                    dictE10[temp[0]] = entry[3]
                    datesCounter[temp[0]] = 1
                if temp[0] in datesCounter:
                    dictDiesel[temp[0]] += entry[1]
                    dictE5[temp[0]] += entry[2]
                    dictE10[temp[0]] += entry[3]
                    datesCounter[temp[0]] += 1

        # Tagespreise werden gemittelt (durch die Anzahl der Einträge für den jeweiligen Tag)
        for i in range(1, 8, 1):
            dictDiesel[strLast7Days[i - 1]] = dictDiesel[strLast7Days[i - 1]] / datesCounter[strLast7Days[i - 1]]
            dictE5[strLast7Days[i - 1]] = dictE5[strLast7Days[i - 1]] / datesCounter[strLast7Days[i - 1]]
            dictE10[strLast7Days[i - 1]] = dictE10[strLast7Days[i - 1]] / datesCounter[strLast7Days[i - 1]]

        # Achsen werden mit Daten befüllt für Diesel, E5 und E10
        achseX_temp = []
        achseY_diesel_temp = []
        achseY_e5_temp = []
        achseY_e10_temp = []
        for entry in datesCounter:
            achseX_temp.append((datetime.strptime(entry, "%Y-%m-%d").strftime("%a \n %d.%m")))
            achseX = list(reversed(achseX_temp))
            achseY_diesel_temp.append(dictDiesel[entry])
            achseY_diesel = list(reversed(achseY_diesel_temp))
            achseY_e5_temp.append((dictE5[entry]))
            achseY_e5 = list(reversed(achseY_e5_temp))
            achseY_e10_temp.append(dictE10[entry])
            achseY_e10 = list(reversed(achseY_e10_temp))

        # Löscht die vorherigen intern gespeicherten Plots
        plt.clf()
        plt.cla()

        # Plotten der Diagramme
        plt.plot(achseX, achseY_diesel, 'ok-', label='Diesel')
        plt.plot(achseX, achseY_e5, 'ob-', label='Benzin E5')
        plt.plot(achseX, achseY_e10, 'og-', label='Benzin E10')
        plt.legend(loc='upper right')
        plt.grid(which='both')
        plt.title('Preisverlauf der letzten 7 Tage')
        plt.xticks(fontsize='8')
        plt.xlabel('Tag')
        plt.ylabel('Preis €/l')
        bildname = 'assets/ChartLast7Days' + str(count) + '.png'
        plt.savefig(bildname)



def createChart_last14Days(ID, today, count):
    # Prüfen, ob Today als String oder Zeitstempel übergeben wurde
    if isinstance(today, str):
        print("STR TO DATETIME")
        today = datetime.strptime(today, '%Y-%m-%d')

    # Ermittlung der letzten 14 Tage
    strLast14Days = []
    today = today + timedelta(days=1)
    for i in range(1, 15, 1):
        subDays = today - timedelta(days=i)
        strLast14Days.append(subDays.strftime('%Y-%m-%d'))

    # Verbindung zur Datenbank herstellen
    connection = sql.connect('benzin_db.sqlite')
    c = connection.cursor()

    # Auslesen der Daten (14*50 Datensätze, da die Anzahl der erfassten Daten je Tag unterschiedlich sind)
    c.execute("SELECT date, diesel, e5, e10 FROM prices WHERE station_uuid = (?) ORDER BY date DESC LIMIT 700", (ID,))
    fetchedData = c.fetchall()

    # Verbindung zur Datenbank beenden
    connection.close()

    # Prüfen, ob die Datensätze in der Datenbank mit dem übergebenen aktuellem Datum übereinstimmen
    checkToday = list(fetchedData[0])[0].split(' ')[0]
    if checkToday == strLast14Days[0]:
        print("CHECK OK")
        status = 'ok'
    else:
        print("CHECK FAILED!")
        status = 'failed'
        error = "Das angegebene Datum stimmt nicht mit den Datensätzen in der Datenbank überein!"

    if status == 'ok':
        # Bereinigung der Daten
        # Abgleich, ob der Eintrag zu den letzten 14 Tagen gehört
        # Abgleich, ob für den Tag bereits ein erster Eintrag vorliegt
        # Wenn nicht, wird er angelegt
        # Wenn ja, wird der Preis aufsummiert
        # Zusätzlich wird die Anzahl der Einträge für die einzelnen Tage ermittelt
        datesCounter = {}
        dictDiesel = {}
        dictE5 = {}
        dictE10 = {}
        for entry in fetchedData:
            temp = entry[0].split(' ')
            if temp[0] in strLast14Days:
                if temp[0] not in datesCounter:
                    dictDiesel[temp[0]] = entry[1]
                    dictE5[temp[0]] = entry[2]
                    dictE10[temp[0]] = entry[3]
                    datesCounter[temp[0]] = 1
                if temp[0] in datesCounter:
                    dictDiesel[temp[0]] += entry[1]
                    dictE5[temp[0]] += entry[2]
                    dictE10[temp[0]] += entry[3]
                    datesCounter[temp[0]] += 1

        # Tagespreise werden gemittelt (durch die Anzahl der Einträge für den jeweiligen Tag)
        for i in range(1, 15, 1):
            dictDiesel[strLast14Days[i - 1]] = dictDiesel[strLast14Days[i - 1]] / datesCounter[strLast14Days[i - 1]]
            dictE5[strLast14Days[i - 1]] = dictE5[strLast14Days[i - 1]] / datesCounter[strLast14Days[i - 1]]
            dictE10[strLast14Days[i - 1]] = dictE10[strLast14Days[i - 1]] / datesCounter[strLast14Days[i - 1]]

        # Achsen werden mit Daten befüllt für Diesel, E5 und E10
        achseX_temp = []
        achseY_diesel_temp = []
        achseY_e5_temp = []
        achseY_e10_temp = []
        for entry in datesCounter:
            achseX_temp.append((datetime.strptime(entry, "%Y-%m-%d").strftime("%a \n %d.%m")))
            achseX = list(reversed(achseX_temp))
            achseY_diesel_temp.append(dictDiesel[entry])
            achseY_diesel = list(reversed(achseY_diesel_temp))
            achseY_e5_temp.append((dictE5[entry]))
            achseY_e5 = list(reversed(achseY_e5_temp))
            achseY_e10_temp.append(dictE10[entry])
            achseY_e10 = list(reversed(achseY_e10_temp))

        # Löscht die vorherigen intern gespeicherten Plots
        plt.clf()
        plt.cla()

        # Plotten der Diagramme
        plt.plot(achseX, achseY_diesel, 'ok-', label='Diesel')
        plt.plot(achseX, achseY_e5, 'ob-', label='Benzin E5')
        plt.plot(achseX, achseY_e10, 'og-', label='Benzin E10')
        plt.legend(loc='upper right')
        plt.grid(which='both')
        plt.title('Preisverlauf der letzten 14 Tage')
        plt.xticks(fontsize='7')
        plt.xlabel('Tag')
        plt.ylabel('Preis €/l')
        bildname = 'assets/ChartLast14Days' + str(count) + '.png'
        plt.savefig(bildname)


def read_application_properties(file, section, key):
    # Application Properties ((application.ini) auslesen
    # Quelle: https://zetcode.com/python/configparser/
    import configparser

    config = configparser.ConfigParser()
    config.read(file)
    value = config[section][key]

    return value


def createChart_BestTime(ID, today, gasType, count):
    # Prüfen, ob Today als String übergeben wurde
    if isinstance(today, str):
        print("STR TO DATETIME.DATE")
        today = datetime.date(datetime.strptime(today, '%Y-%m-%d'))

    # Prüfen, ob Today als Datetime.Datetime übergeben wurde
    if isinstance(today, datetime):
        print("DATETIME.DATETIME TO DATETIME.DATE")
        today = datetime.date(today)
    today = today + timedelta(days=1)

    # Prüfen, ob Tanksorte richtig übergeben wurde
    gasTypes = ["E5", "E10", "Diesel"]
    if gasType in gasTypes:
        status = 'ok'
    else:
        print("CHECK FAILED!")
        status = 'failed'
        error = "Das Tanksorte wurde nicht korrekt angegeben! ('E5', 'E10' oder 'Diesel')"

    if status == 'ok':
        # Aktuelles Datum
        aktuellesDatum = today

        # Zeitraum für Chart für besten Tankzeitpunkt in Wochen
        chart_zeitraum = read_application_properties(file, "download", "chart_zeitraum")

        # Zeitraum in Wochen in Tagen
        chart_tage = timedelta(weeks=int(chart_zeitraum))

        # Anzahl der Tage für die For Schleife
        tage_bis = int(read_application_properties(file, "download", "chart_zeitraum")) * 7
        chart_date = aktuellesDatum - chart_tage

        data_bestTime = []

        if gasType == 'E5':
            tank_art = 1
        elif gasType == 'E10':
            tank_art = 2
        elif gasType == 'Diesel':
            tank_art = 3

        # Datenbankname aus Application Properties lesen
        db_name = read_application_properties(file, "database", "db_source")
        connection = sql.connect(db_name)
        c = connection.cursor()  # Ein Curor Objekt mit der cursor() Methode

        i = 1
        while i <= tage_bis:
            # SQL für e5
            if tank_art == 1:
                sqlstring = "SELECT  (substr(date,1,10)),(substr(date,12,2)),min(e5) FROM prices " \
                            "where station_uuid=? and (substr(date,1,10))=?"
                c.execute(sqlstring, (ID, chart_date,))
            # SQL für e10
            elif tank_art == 2:
                c.execute(
                    "SELECT  (substr(date,1,10)),(substr(date,12,2)),min(e10) FROM prices "
                    "where station_uuid=? and (substr(date,1,10))=?",
                    (ID, chart_date,))
            # SQL für Diesel
            elif tank_art == 3:
                c.execute(
                    "SELECT  (substr(date,1,10)),(substr(date,12,2)),min(diesel) FROM prices "
                    "where station_uuid=? and (substr(date,1,10))=?",
                    (ID, chart_date,))
            fetchedData = c.fetchall()

            for r in fetchedData:
                weekday = chart_date.strftime('%A')
                if tank_art == 1:
                    data_temp = [r[0], r[1], r[2], weekday, 'e5']
                    if data_temp not in data_bestTime:
                        data_bestTime.append(data_temp)
                elif tank_art == 2:
                    data_temp = [r[0], r[1], r[2], weekday, 'e10']
                    if data_temp not in data_bestTime:
                        data_bestTime.append(data_temp)
                if tank_art == 3:
                    data_temp = [r[0], r[1], r[2], weekday, 'diesel']
                    if data_temp not in data_bestTime:
                        data_bestTime.append(data_temp)

            tag = timedelta(days=1)
            chart_date = chart_date + tag
            i += 1
        c.close()

        # Erstellung eines Charts als Heatmap
        # Quelle: https://matplotlib.org/stable/gallery/images_contours_and_fields/
        #         image_annotated_heatmap.html#sphx-glr-gallery-images-contours-and-fields-image-annotated-heatmap-py

        chartData = np.zeros([7, 6])
        countMo = 0
        countDi = 0
        countMi = 0
        countDo = 0
        countFr = 0
        countSa = 0
        countSo = 0

        for entry in data_bestTime:
            if entry[3] == 'Montag':
                chartData[0][countMo] = entry[1]
                countMo += 1
            if entry[3] == 'Dienstag':
                chartData[1][countDi] = entry[1]
                countDi += 1
            if entry[3] == 'Mittwoch':
                chartData[2][countMi] = entry[1]
                countMi += 1
            if entry[3] == 'Donnerstag':
                chartData[3][countDo] = entry[1]
                countDo += 1
            if entry[3] == 'Freitag':
                chartData[4][countFr] = entry[1]
                countFr += 1
            if entry[3] == 'Samstag':
                chartData[5][countSa] = entry[1]
                countSa += 1
            if entry[3] == 'Sonntag':
                chartData[6][countSo] = entry[1]
                countSo += 1

        # Löscht die vorherigen intern gespeicherten Plots
        plt.clf()
        plt.cla()

        # Plotten der Diagramme
        fig, ax = plt.subplots()
        im = ax.imshow(chartData)

        # Festlegung der X- und Y-Achsen
        ax.set_xticks(np.arange(6))
        ax.set_xticklabels(["vor 6 W.", "vor 5 W.", "vor 4 W.", "vor 3 W.", "vor 2 W.", "letzte W."])
        ax.set_yticks(np.arange(7))
        ax.set_yticklabels(["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"])

        # Rotation der X-Achsenbeschriftung
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
                 rotation_mode="anchor")

        # Einfügen der Uhrzeiten als Beschriftungen in das Diagramm
        for i in range(7):
            for j in range(6):
                text = ax.text(j, i, f" {int(chartData[i, j])} Uhr",
                               ha="center", va="center", color="black")

        ax.grid(visible=True, which="minor", color="black", linestyle='-', linewidth=3)
        ax.figure.colorbar(im, ax=ax, format="%.0f Uhr")
        ax.set_title(f"Beste Tankzeitpunkte der letzten 6 Wochen für {gasType}")
        fig.tight_layout()
        bildname = 'assets/ChartBestTime' + str(count) + '.png'
        plt.savefig(bildname)


def get_max_date(uuid):
    # Das aktuellste Datum aus der Tabelle prices holen für die Diagramme
    # Datenbankname aus Application Properties lesen
    db_name = read_application_properties(file, "database", "db_source")
    m_date = ""
    # Verbindung DB
    connection = sql.connect(db_name)
    c = connection.cursor()  # Ein Curor Objekt mit der cursor() Methode
    # SQL für das aktuellste Datum in der Tabelle prices
    sqlstring = "SELECT  max(date) FROM prices where station_uuid=?"
    # Auführung SQL
    c.execute(sqlstring, (uuid,))
    fetchedData = c.fetchall()
    for r in fetchedData:
        m_date = r[0]
        m_date = m_date[0:10]

    # DB Verbindung schließen
    c.close()

    return m_date


'''
####### Funktionstest #######

# Tankstellen-IDs zum Funktionstest
# testID = "dfbcb1cf-0bf1-4d03-be32-eb2fd463a7fc"
# testID = "7b029caf-172e-4e50-6300-f2991c068432"

# Datumsangaben zum Funktionstest
# today = datetime.today()
today = '2022-01-05'

# Gastyp zum Funktionstest
gasType = 'Diesel'

# Ausgabe der zum Test verwendeten Parameter
print("TestID:", testID)
print("Akt. Datum:", today)
print("Gewählter Gastyp:", gasType)

# Funktionstest 'createChart_LastDay'
createChart_lastDay(testID, today,0)
# Funktionstest 'createChart_Last7Days'
createChart_last7Days(testID, today,0)
# Funktionstest 'createChart_Last14Days'
createChart_last14Days(testID, today,0)
# Funktionstest 'createChart_BestTime'
createChart_BestTime(testID, today, gasType,0)
'''
