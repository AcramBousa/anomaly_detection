from sklearn.cluster import OPTICS
from pandas import np
#from core.anomaly_detection import AnomalyDetection


class OPTICSAnomalyDetection():  # Added relation with Anomaly Detection

    eps = 0.1                                    #added eps parameter inside class

    def anomaly_detection(self, dataframe):     # changed function name with anomaly_detection

        self.df = dataframe
        model = OPTICS(eps=self.eps, cluster_method='dbscan', metric='minkowski')


        x = self.df.copy()
        self.df.drop(columns=["PODID", "EPID", "end_time", "TownCode"], inplace=True)
        x.drop(columns=["ActiveEnergy", "dayORnight", "end_time"], inplace=True)

        model.fit(self.df)
        labels = model.labels_
        no_clusters = len(np.unique(labels))
        no_noise = list(labels).count(-1)

        print('Estimated number of clusters: %d' % no_clusters)
        print('Estimated number of noise points: %d' % no_noise)

        outliers_DF = self.df[labels == -1]

        outliers_DF = outliers_DF.join(x)       # Added join from x to outliers_DF in order to display PODID, EPID
        outliers_DF.drop(columns=["dayORnight"], inplace=True)
        print("OPTICS")
        return outliers_DF




