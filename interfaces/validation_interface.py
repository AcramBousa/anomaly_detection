class ValidationInterface:
    def validate(self, synthetic_anomalies, detected_anomalies):
        raise NotImplementedError()