from adapters.synthetic_load_adapter import SyntheticAnomalies
from core.anomaly_detection import AnomalyDetection
from adapters.validation_adapter import Validator
from adapters.data_plotter import DataPlotter

class ValidatorInterface:
    def __init__(self, data_adapter):
        self.data_adapter = data_adapter
        self.plotter_adapter = DataPlotter()

    def validate_data(self, file_path, podid, algorithm):

        # Carica i dati dal file
        data = self.data_adapter.load_jsons(path=file_path, podid=podid)

        synth = SyntheticAnomalies()

        # Inserisce le anomalie fittizie sulla base dello scenario
        data_with_anomalies = synth.synthetic_anomalies(data, 1)

        # Crea l'istanza del core con l'algoritmo selezionato
        anomaly_detection = AnomalyDetection(algorithm)

        # Esegue il rilevamento delle anomalie
        anomalies = anomaly_detection.anomaly_detection(data_with_anomalies)

        validator = Validator()
        # validazione
        v = validator.validate(synth, anomalies)
        print(v)

        # Salva grafico con le distribuzioni delle anomalie
        self.plotter_adapter.plot_data(anomalies, algorithm)
