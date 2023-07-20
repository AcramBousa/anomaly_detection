#from pandas import np
import numpy as np
from sklearn.cluster import DBSCAN
#from implementation.PELL_street_lighting_data import DynamicDataManager
#from core.anomaly_detection import AnomalyDetection
#from synthetic_anomaly_injection import SyntheticAnomalies


class DBSCANAnomalyDetection(): #Added relation with Anomaly Detection

    eps = 0.35                                  # added eps parameter inside class
    min_samples = 12                            # added min_samples parameter inside class

    def anomaly_detection(self, dataframe):                # changed function name with anomaly_detection

        self.df = dataframe

        # begin_time = time.time()
        db = DBSCAN(eps=self.eps, min_samples=self.min_samples)

        #ADDED COPY TO JOIN MORE COLUMNS
        x = self.df.copy()
        self.df.drop(columns=["PODID", "EPID", "end_time", "TownCode"], inplace=True)
        x.drop(columns=["ActiveEnergy", "dayORnight", "end_time"], inplace=True)

        print("DBSCAN DF")
        print(self.df)
        db.fit(self.df)
        labels = db.labels_

        # Creating a numpy array with all values set to false by default
        core_samples_mask = np.zeros_like(labels, dtype=bool)

        # Setting core and border points (all points that are not -1) to True
        core_samples_mask[db.core_sample_indices_] = True

        # Number of clusters in labels, ignoring noise if present.
        n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise_ = list(labels).count(-1)

        # end_time = time.time()
        # print('Elapsed time is %f seconds.' % (time.time() - begin_time))

        print('Estimated number of clusters: %d' % n_clusters_)
        print('Estimated number of noise points: %d' % n_noise_)

        # outlier dataframe
        outliers_DF = self.df[db.labels_ == -1]

        outliers_DF = outliers_DF.join(x)       # Added join from x to filtered_anomaly in order to display also PODID
        outliers_DF.drop(columns=["dayORnight"], inplace=True)
        print("DBSCAN")
        return outliers_DF
