class SyntheticAnomaly:
    def __init__(self, anomaly_injector):
        self.anomaly_injector = anomaly_injector

    def inject_anomalies(self, dataframe, scenario):
        anomalies = self.anomaly_injector.synthetic_anomalies(dataframe, scenario)
        return anomalies