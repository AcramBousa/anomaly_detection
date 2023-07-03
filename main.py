import pandas as pd

#from modules.anomaly_detection_module import AnomalyDetection
from core.anomaly_detection import AnomalyDetection
from adapters.data_load_adapter import DynamicDataManager
from adapters.data_plotter import DataPlotter
from adapters.synthetic_load_adapter import SyntheticAnomalies
from adapters.validation_adapter import Validator
from main_interface.main_interface import MainInterface
from main_interface.validator_interface import ValidatorInterface
from adapters.database_adapter import SQLDatabaseGateway



def main():

    #SETTING VARIABILI SETUP

    path = 'PELL_Data/Genova'
    podid = 'IT001E00097610'            # GENOVA PODID TO TEST: IT001E00097688, IT001E00097554,IT001E00097963,IT001E00097639,IT001E00122846, IT001E00097610,
    epid = None
    towncode = None
    start_t = None                            # '2020-09-01 00:00:00'
    end_t = None                              # '2020-09-15 23:59:59'


    # Crea le istanze degli adattatori
    data_adapter = DynamicDataManager()
    database_adapter = SQLDatabaseGateway()

    # Crea l'interfaccia utente e avvia il programma
    main_interface = MainInterface(data_adapter, database_adapter)

    # Crea l'interfaccia utente per la validazione
    validator_interface = ValidatorInterface(data_adapter)

    # Richiedi all'utente di selezionare l'algoritmo
    algorithm_choice = 'optics'                 # input("Seleziona l'algoritmo (kmeans, dbscan o optics): ")

    #''' Avvia il programma con l'algoritmo selezionato e manda anomalie rilevate a db
    main_interface.run(path,
                       podid,
                       epid,
                       towncode,
                       start_t,
                       end_t,
                       algorithm_choice)
    #'''
    # Avvia il programma di validazione, mostrando metriche e salvando grafo anomalie
    #validator_interface.validate_data(path, podid, algorithm_choice)

def show_result(filtered_df):
    pd.set_option('display.max_rows', 9000)
    print(filtered_df.head(18000))


if __name__ == "__main__":
    main()



