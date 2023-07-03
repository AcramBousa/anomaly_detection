import pandas as pd
import requests

# Esempi di richieste alle API REST
def test_api():

    # Esempio di richiesta POST per caricare i dati
    url_data = "http://localhost:5000/data"
    payload_data = {
        "filename": "..\PELL_Data\\Genova"
    }
    response = requests.post(url_data, json=payload_data)
    data = response.json()

    print(pd.read_json(data["dataframe"]))

    # Esempio di richiesta POST per rilevare le anomalie
    url_anomalies = "http://localhost:5000/anomalies"
    payload_anomalies = {
        "dataframe": data["dataframe"]  # Utilizza il dataframe restituito dalla richiesta precedente
    }
    response = requests.post(url_anomalies, json=payload_anomalies)
    anomalies = response.json()
    print(pd.read_json(anomalies["anomalies"]))


    #'''
    # Esempio di richiesta POST per scrivere i dati nel database
    url_write = "http://localhost:5000/write"
    payload_write = {
        "anomalies": anomalies["anomalies"]  # Utilizza le anomalie restituite dalla richiesta precedente
    }
    response = requests.post(url_write, json=payload_write)
    result = response.json()
    print(result)
    #'''
# Esegui il test delle API
test_api()
