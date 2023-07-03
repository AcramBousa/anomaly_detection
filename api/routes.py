import pandas as pd
from flask import jsonify, request

from modules.anomaly_detection_module import AnomalyDetection
from modules.database_gateway_module import DatabaseGateway
from adapters.data_load_adapter import DynamicDataManager
from core.algorithms.Kmeans_anomaly_detection import KmeansAnomalyDetection
from core.algorithms.dbscan_anomaly_detection import DBSCANAnomalyDetection
from adapters.database_adapter import SQLDatabaseGateway

def register_routes(app):

    @app.route("/data", methods=["POST"])
    def load_data():
        # Caricamento dei dati dal JSON
        data_loader = DynamicDataManager()
        filename = request.json["filename"]
        dataframe = data_loader.load_jsons(filename)
        print(dataframe)
        return jsonify({"message": "Dati caricati", "dataframe": dataframe.to_json()})

    @app.route("/anomalies", methods=["POST"])
    def detect_anomalies():
        # Rilevazione delle anomalie
        anomaly_detector = DBSCANAnomalyDetection()
        anomaly_detection = AnomalyDetection(anomaly_detector)
        # Otteniamo il dataframe dal payload della richiesta
        dataframe_json = request.json["dataframe"]
        dataframe = pd.read_json(dataframe_json)
        anomalies = anomaly_detection.detect_anomalies(dataframe)
        print(anomalies)
        return jsonify({"message": "Rilevazione anomalie eseguite", "anomalies": anomalies.to_json()})

    @app.route("/write", methods=["POST"])
    def write_to_database():
        try:
            # Scrittura sul database
            database_writer = SQLDatabaseGateway()
            database_gateway = DatabaseGateway(database_writer)
            anomalies_json = request.json["anomalies"]
            anomalies = pd.read_json(anomalies_json)
            database_gateway.write_to_database(anomalies)

            return jsonify({"message": "Data written to database successfully"})
        except Exception as e:
            error_message = str(e)
            return jsonify({"error": error_message})