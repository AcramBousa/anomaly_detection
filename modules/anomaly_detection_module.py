class AnomalyDetection:
    def __init__(self, anomaly_detector):
        self.anomaly_detector = anomaly_detector

    def detect_anomalies(self, dataframe):
        anomalies = self.anomaly_detector.anomaly_detection(dataframe)
        return anomalies
