from core.algorithms.Kmeans_anomaly_detection import KmeansAnomalyDetection
from core.algorithms.dbscan_anomaly_detection import DBSCANAnomalyDetection
from core.algorithms.OPTICS_anomaly_detection import OPTICSAnomalyDetection

class AnomalyDetection:
    def __init__(self, algorithm):
        self.algorithm = algorithm

    def anomaly_detection(self, dataframe):
        # Seleziona l'algoritmo in base alla scelta effettuata
        if self.algorithm == 'kmeans':
            algorithm_instance = KmeansAnomalyDetection()
        elif self.algorithm == 'dbscan':
            algorithm_instance = DBSCANAnomalyDetection()
        elif self.algorithm == 'optics':
            algorithm_instance = OPTICSAnomalyDetection()
        else:
            raise ValueError("Algoritmo non supportato")

        # Esegue il rilevamento delle anomalie utilizzando l'algoritmo selezionato
        anomalies = algorithm_instance.anomaly_detection(dataframe)
        return anomalies
