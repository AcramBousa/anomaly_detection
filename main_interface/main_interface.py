from core.anomaly_detection import AnomalyDetection

from adapters.data_plotter import DataPlotter

class MainInterface:
    def __init__(self, data_adapter, database_adapter):
        self.data_adapter = data_adapter
        self.database_adapter = database_adapter

    def run(self, file_path, podid, epid, towncode, start_t, end_t, algorithm):

        #print(file_path)

        # Carica i dati dal file
        data = self.data_adapter.load_jsons(path=file_path,
                                            podid = podid,
                                            epid=epid,
                                            towncode=towncode,
                                            start_t=start_t,
                                            end_t=end_t)

        # Crea l'istanza del core con l'algoritmo selezionato
        anomaly_detection = AnomalyDetection(algorithm)

        # Esegue il rilevamento delle anomalie
        anomalies = anomaly_detection.anomaly_detection(data)

        # Stampa a schermo le anomalie rilevate
        print(anomalies)

        # Salva le anomalie nel database
        #self.database_adapter.write_to_database(anomalies)

        plotter_adapter = DataPlotter()
        # Salva il grafico di distribuzione delle anomalie
        #plotter_adapter.plot_data(anomalies, algorithm)



