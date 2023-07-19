from interfaces.validation_interface import ValidationInterface


class Validator(ValidationInterface):
    def validate(self, synthetic_anomalies, detected_anomalies):
        #print(synthetic_anomalies.list_of_anomalies)
        #print(synthetic_anomalies.random_rows)
        tp = 0
        ne = 0
        for i in range(synthetic_anomalies.number_of_anomalies):
            try:
                if detected_anomalies.loc[synthetic_anomalies.random_rows[i], 'ActiveEnergy'] == synthetic_anomalies.list_of_anomalies[i]:
                    tp = tp + 1
            except:
                ne = ne + 1
                #print(ne)

        #FP: numeri di anomalie positivi al rilevamento ma non tra quelle inserite manualmente
        fp = len(detected_anomalies) - tp
        #FN: numero di anomalie interite manualmente ma negativi al rilevamento
        fn = synthetic_anomalies.number_of_anomalies - tp

        print("NUMBER OF ANOMALIES")
        print(synthetic_anomalies.number_of_anomalies)
        print("TRUE POSITIVE")
        print(tp)

        #Calculation of the metrics
        print("PRECISION")
        precision = tp / (tp + fp)
        print(precision)

        print("RECALL")
        recall = tp / (tp + fn)
        print(recall)

        print("F1-Score")
        f1_score = 2 * (precision * recall) / (precision + recall)
        print(f1_score)

        #print("LIST OF ANOMALIES")
        #print(synthetic_anomalies.list_of_anomalies)



        metrics = [precision, recall, f1_score]
        return metrics
