import kubernetes
import time
import random
import logging
import sys
import psycopg
import watttime
import dotenv
import os

# Konfiguration von Umgebungsvariablen aus .env und .env.local Dateien
dotenv.load_dotenv()
if dotenv.find_dotenv(".env.local"):
    dotenv.load_dotenv(".env.local")

# Konfiguration eines Loggers für die Ausgabe von Meldungen
logging.basicConfig(stream= sys.stdout, level= logging.INFO)
logger = logging.getLogger(__name__)

# Laden der Kubernetes API Konfiguration
kubernetes.config.load_kube_config(
    os.getenv("KUBE_CONFIG_PATH")
)

# Festlegen des Dry-Run-Modus zum Testen
# - Auf "None" setzen, um die Operationen tatsächlich auszuführen
# - Auf "All" setzen, um die Operationen nur zu simulieren
dry_run = os.getenv("DRY_RUN")

# Festlegen, ob der Operator simuliert werden soll, ohne tatsächlich Nodes zu starten oder zu stoppen
# Dadurch können Vergleichswerte für die CO2-Emissionswerte der Nodes generiert werden
simulate_no_operator = False

# Zugangsdaten für die Datenbankverbindung
db_connection_string = os.getenv("DB_CONNECTION_STRING")

# Zugangsdaten für die WattTime API
watttime_api_username = os.getenv("WATTTIME_API_USERNAME")
watttime_api_password = os.getenv("WATTTIME_API_PASSWORD")

# Platzhalter für die Speicherung der CO2-Emissionswerte der Nodes
# Notwendig zur Simulation der MOER-Werte, da API-Regionen der WattTime API nicht ausreicht
node_moer_values = {}

ignored_node_names = os.getenv("IGNORED_NODE_NAMES", "").split(",")

start_time = time.time()

def get_insert_timestamp():
    """
    Gibt den aktuellen Zeitstempel im Format "YYYY-MM-DD HH:MM:SS" zurück.
    """
    return time.strftime('%Y-%m-%d %H:%M:%S')

def setup_database(db: psycopg.Connection):
    """
    Erstellt die benötigte Datenbank und Tabellen für die Speicherung von Node-Informationen und CO2-Emissionswerten.
    """

    # Erstellen eines Datenbank-Cursors für die Ausführung von SQL-Abfragen
    cursor = db.cursor()

    # Löschen und Erstellen der Tabellen
    cursor.execute("DROP TABLE IF EXISTS node_infos")
    cursor.execute("CREATE TABLE node_infos (node_name VARCHAR(255), lat FLOAT, lng FLOAT)")

    cursor.execute("DROP TABLE IF EXISTS node_metric_entries")
    cursor.execute("DROP TYPE IF EXISTS value_type")
    cursor.execute("CREATE TYPE value_type AS ENUM ('MOER', 'POWER')")
    cursor.execute("CREATE TABLE node_metric_entries (node_name VARCHAR(255), value_type value_type, timestamp TIMESTAMP, value FLOAT)")

    # Commit der Änderungen an der Datenbank
    db.commit()

def get_node_moer_value(node: kubernetes.client.V1Node, wt_api: watttime.WattTimeForecast, db: psycopg.Connection):
    """
    Berechnet die CO2-Emissionsrate für einen Node.
    """

    lat_lng = get_node_latlng(node, db)

    # Standardmässig wird der MOER-Wert auf 50 simuliert, sofern er noch nicht existiert
    if not node.metadata.name in node_moer_values:
        node_moer_values[node.metadata.name] = 50

    # Bei jedem Abruf wird der MOER-Wert zufällig um 1 oder 2 erhöht oder verringert
    # Dabei darf er 25-75 nicht überschreiten
    moer_value = node_moer_values[node.metadata.name] + random.choice([-4, -2, 2, 4])
    moer_value = max(25, min(75, moer_value))

    # Tatsächliche Berechnung des CO2-Emissionswerts (API-Zugriff nicht ausreichend):
    # region_info = wt_api.region_from_loc(
    #     signal_type= "co2_moer",
    #     latitude= lat_lng["lat"],
    #     longitude= lat_lng["lng"]
    # )

    # forecast = wt_api.get_forecast_json(
    #     region= region_info["region"],
    #     signal_type= "co2_moer",
    #     horizon_hours= 0
    # )

    # moer_value = forecast["data"][0]["value"]

    # Erstellen eines Datenbank-Cursors für die Ausführung von SQL-Abfragen
    cursor = db.cursor()

    # Speichern der CO2-Emissionsrate in der Datenbank
    cursor.execute(
        "INSERT INTO node_metric_entries (node_name, timestamp, value_type, value) VALUES (%s, %s, 'MOER', %s)",
        (node.metadata.name, get_insert_timestamp(), moer_value)
    )

    # Commit der Änderungen an der Datenbank
    db.commit()

    return moer_value

def get_node_latlng(node: kubernetes.client.V1Node, db: psycopg.Connection):
    """
    Ermittelt die geografischen Koordinaten (Breitengrad und Längengrad) eines Nodes.
    
    Wenn sie bereits in der Datenbank existieren, werden die dort gespeicherten Werte zurückgegeben.
    """

    # Erstellen eines Datenbank-Cursors für die Ausführung von SQL-Abfragen
    cursor = db.cursor()

    # Abrufen der geografischen Koordinaten aus der Datenbank, falls vorhanden
    node_info = cursor.execute("SELECT lat, lng FROM node_infos WHERE node_name = %s", (node.metadata.name,)).fetchone()

    # Wenn die geografischen Koordinaten in der Datenbank vorhanden sind, werden sie zurückgegeben
    if node_info is not None:
        return {"lat": node_info[0], "lng": node_info[1]}

    # Platzhalter für die tatsächliche Ermittlung der geografischen Koordinaten
    # Die Zahlen enthalten ungefähr den Bereich Europas
    new_lat_lng = {
        "lat": random.uniform(34.5, 71.2),
        "lng": random.uniform(-31.3, 42.0)
    }

    # Speichern der geografischen Koordinaten in der Datenbank
    cursor.execute("INSERT INTO node_infos (node_name, lat, lng) VALUES (%s, %s, %s)", (node.metadata.name, new_lat_lng["lat"], new_lat_lng["lng"]))

    # Commit der Änderungen an der Datenbank
    db.commit()

    return new_lat_lng

def is_node_running(node_name: str, db: psycopg.Connection):
    """
    Prüft, ob ein Node im Cluster läuft.
    Platzhalter für die tatsächliche Überprüfung, ob der Node läuft
    """

    # Erstellen eines Datenbank-Cursors für die Ausführung von SQL-Abfragen
    cursor = db.cursor()

    # Letzte "POWER"-Metrik des Nodes abrufen
    last_power_metric = cursor.execute("SELECT value FROM node_metric_entries WHERE node_name = %s AND value_type = 'POWER' ORDER BY timestamp DESC LIMIT 1", (node_name,)).fetchone()

    # Wenn keine "POWER"-Metrik vorhanden ist, wird eine eingetragen und angenommen, dass der Node läuft
    if last_power_metric is None:
        cursor.execute("INSERT INTO node_metric_entries (node_name, timestamp, value_type, value) VALUES (%s, %s, 'POWER', 1)", (node_name, get_insert_timestamp()))

        db.commit()
        return True
    
    # Wenn die letzte "POWER"-Metrik größer als 0 ist, wird angenommen, dass der Node läuft
    return last_power_metric[0] > 0

def start_node(node_name: str, db: psycopg.Connection):
    """
    Startet einen Node im Cluster.
    Platzhalter für den tatsächlichen Node-Startvorgang
    """

    logger.info(f"Starting node {node_name}")
    
    # Erstellen eines Datenbank-Cursors für die Ausführung von SQL-Abfragen
    cursor = db.cursor()

    # Eintragen einer "POWER"-Metrik für den Node, um anzuzeigen, dass er läuft
    cursor.execute("INSERT INTO node_metric_entries (node_name, timestamp, value_type, value) VALUES (%s, %s, 'POWER', 1)", (node_name, get_insert_timestamp()))

    # Commit der Änderungen an der Datenbank
    db.commit()

def stop_node(node_name: str, db: psycopg.Connection):
    """
    Stoppt einen Node im Cluster.
    Platzhalter für den tatsächlichen Node-Stoppvorgang
    """

    logger.info(f"Stopping node {node_name}")

    # Erstellen eines Datenbank-Cursors für die Ausführung von SQL-Abfragen
    cursor = db.cursor()

    # Eintragen einer "POWER"-Metrik für den Node, um anzuzeigen, dass er nicht mehr läuft
    cursor.execute("INSERT INTO node_metric_entries (node_name, timestamp, value_type, value) VALUES (%s, %s, 'POWER', 0)", (node_name, get_insert_timestamp()))

    # Commit der Änderungen an der Datenbank
    db.commit()

def wait_for_eviction(k8s_api: kubernetes.client.CoreV1Api, node_name: str, timeout= 300):
    """
    Wartet auf die Evakuierung aller Pods von einem Node.

    Wenn timeout erreicht wird und noch Pods auf dem Node laufen, wird False zurückgegeben.
    """

    # Berechnen der Endzeit basierend auf dem Timeout
    end_time = time.time() + timeout

    # Warten, bis alle Pods vom Node evakuiert sind oder der Timeout erreicht ist
    while time.time() < end_time:
        # Abrufen aller Pods, die auf dem Node ausgeführt werden
        pods: kubernetes.client.V1Pod = k8s_api.list_pod_for_all_namespaces(field_selector= f"spec.nodeName={node_name}").items

        # Wenn keine Pods mehr auf dem Node laufen, wird True zurückgegeben
        if not pods:
            return True
        
        # Warten für 5 Sekunden, bevor der nächste Versuch unternommen wird
        time.sleep(5)

    # Timeout erreicht, es laufen noch Pods auf dem Node
    return False

def monitor_nodes():
    """
    Überwacht die Nodes im Cluster und optimiert sie basierend auf den CO2-Emissionswerten.
    """

    # Erstellen eines API-Objekts für die Kommunikation mit der Kubernetes API
    k8s_api = kubernetes.client.CoreV1Api()

    # Erstellen eines API-Objekts für die Kommunikation mit der WattTime API
    wt_api = watttime.WattTimeForecast(watttime_api_username, watttime_api_password)

    with psycopg.connect(db_connection_string) as db:

        # Einrichten der Datenbank und Tabellen
        logger.info("Setting up database...")

        setup_database(db)

        logger.info("Database setup complete!")

        while True:
            # Abrufen aller Nodes im Cluster
            nodes: kubernetes.client.V1NodeList = k8s_api.list_node()

            nodes = [node for node in k8s_api.list_node().items if node.metadata.name not in ignored_node_names]

            # Berechnen der CO2-Emissionswerte für alle Nodes
            node_moer_values = {node.metadata.name: get_node_moer_value(node, wt_api, db) for node in nodes}

            if simulate_no_operator:
                logger.info("Skipping operator simulation...")

                # Warten für 5 Minuten (5 * 60 * 1 Sekunde), bevor der nächste Zyklus beginnt
                # Dies ermöglicht ein sofortiges Beenden der Schleife, wenn das Programm beendet wird, ohne dass auf den "sleep" gewartet wird
                for _ in range(5 * 60):
                    time.sleep(1)
                continue

            # Sortieren der Nodes nach ihren CO2-Emissionswerten
            sorted_nodes = sorted(node_moer_values.items(), key=lambda item: item[1])
            
            logger.info(f"Node emission rates: {node_moer_values}")

            nodes_half_len = len(sorted_nodes) // 2 + len(sorted_nodes) % 2

            # Auswählen der Nodes, die für die Ausführung von Pods zulässig sind (erste Hälfte der Liste der Nodes)
            nodes_to_allow = sorted_nodes[:nodes_half_len]
            
            # Auswählen der Nodes, die für die Ausführung von Pods nicht zulässig sind (zweite Hälfte der Liste der Nodes)
            nodes_to_disallow = sorted_nodes[nodes_half_len:]

            # Sicherstellen, dass mindestens ein Node für die Ausführung von Pods zulässig ist
            if len(nodes_to_allow) == 0:
                nodes_to_allow.append(nodes_to_disallow.pop(0))

            # Schleife über die Nodes, die für die Ausführung von Pods zulässig sind
            for node_name, _ in nodes_to_allow:
                logger.info(f"Allowing node {node_name} for pod scheduling")

                # Starten des Nodes, wenn er nicht ausgeführt wird
                if not is_node_running(node_name, db):
                    start_node(node_name, db)

                # Sicherstellen, dass der Node für die Ausführung von Pods zulässig ist
                body = {"spec": {"unschedulable": False}}

                # Änderungen am Node anwenden
                k8s_api.patch_node(node_name, body, dry_run= dry_run)
            
            # Schleife über die Nodes, die für die Ausführung von Pods nicht zulässig sind
            for node_name, _ in nodes_to_disallow:
                logger.info(f"Disallowing node {node_name}")

                # Sichern, dass der Node für die Ausführung von Pods nicht zulässig ist
                body = {"spec": {"unschedulable": True}}

                # Änderungen am Node anwenden
                k8s_api.patch_node(node_name, body, dry_run= dry_run)

                # Auflisten aller Pods, die auf dem Node ausgeführt werden
                pods = k8s_api.list_pod_for_all_namespaces(field_selector= f"spec.nodeName={node_name}")

                # Erstellen einer Evakuierung (Eviction) für jeden Pod auf dem Node
                for pod in pods.items:
                    logger.info(f"Evicting pod {pod.metadata.name}")

                    # Erstellen einer Evakuierung für den Pod
                    eviction_body = kubernetes.client.V1Eviction(
                        metadata= kubernetes.client.V1ObjectMeta(
                            name= pod.metadata.name,
                            namespace= pod.metadata.namespace
                        )
                    )

                    try:
                        # Durchführen der Evakuierung
                        k8s_api.create_namespaced_pod_eviction(
                            name= pod.metadata.name,
                            namespace= pod.metadata.namespace,
                            body= eviction_body,
                            dry_run= dry_run
                        )
                    except kubernetes.client.exceptions.ApiException as e:
                        # Fehlerbehandlung, falls die Evakuierung fehlschlägt
                        logger.error(f"Failed to evict pod {pod.metadata.name}: {e}")

                logger.info(f"Waiting for node {node_name} to be drained...")

                # Warten, bis alle Pods vom Node evakuiert sind
                if dry_run is not None or wait_for_eviction(k8s_api, node_name):
                    logger.info(f"Node {node_name} has been drained")

                    # Stoppen des Nodes, wenn er noch ausgeführt wird
                    if is_node_running(node_name, db):
                        stop_node(node_name, db)

                    logger.info(f"Node {node_name} has been shut down")
                else:
                    # Timeout erreicht, es laufen noch Pods auf dem Node
                    logger.error(f"Timeout while waiting for node {node_name} to be drained")

            logger.info("Completed CO2-based node optimization cycle...beginning next cycle in 5 minutes...")
            
            # Warten für 5 Minuten (5 * 60 * 1 Sekunde), bevor der nächste Zyklus beginnt
            # Dies ermöglicht ein sofortiges Beenden der Schleife, wenn das Programm beendet wird, ohne dass auf den "sleep" gewartet wird
            for _ in range(5 * 60):
                time.sleep(1)

if __name__ == '__main__':
    # Starten des Operators
    monitor_nodes()
