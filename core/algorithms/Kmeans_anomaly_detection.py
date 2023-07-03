from sklearn.cluster import KMeans
from pandas import np
#from implementation.PELL_street_lighting_data import DynamicDataManager
import pandas as pd
#from core.anomaly_detection import AnomalyDetection
#from synthetic_anomaly_injection import SyntheticAnomalies


class KmeansAnomalyDetection():  # Added relation with Anomaly Detection

    no_clusters = 2                              # added no_cluster parameter inside the class

    def anomaly_detection(self, dataframe):             # changed function name with anomaly_detection

        # begin_time = time.time()

        self.df = dataframe

        kmeans = KMeans(n_clusters=self.no_clusters, init='k-means++')

        app = self.df.copy()
        self.df.drop(columns=["PODID", "EPID", "end_time", "TownCode"], inplace=True)
        app.drop(columns=["ActiveEnergy", "dayORnight", "end_time"], inplace=True)

        kmeans.fit(self.df)

        filtered_anomalies = 0

        # Predicting the clusters
        labels = kmeans.predict(self.df)

        # --------------------------->>>>>>>> Distance Calculation <<<<---------------------
        X_dist = kmeans.transform(self.df) ** 2
        # panda_DF = pd.DataFrame(X_dist.sum(axis=1).round(2), columns=['sqdist'])
        dist_sqr = (X_dist.sum(axis=1).round(2))
        self.df['sqr_dist'] = dist_sqr
        self.df['cluster'] = kmeans.labels_

        #print(panda_DF.head(10))

        # ---------------- Anomaly score calculation with respect to each cluster --------------

        k = [0, 1]
        for x in k:
            pd.options.mode.chained_assignment = None
            filtered_DF = self.df[(self.df['cluster'] == k.index(x))]

            # print(filtered_DF)
            # print(filtered_DF.describe())


            # calculating the mean of distance column
            filtered_DF['mean_dist'] = filtered_DF['sqr_dist'].mean()

            # Calculating the anomaly score by dividing distance column by mean_distance column
            filtered_DF['anomaly_score'] = filtered_DF['sqr_dist'] / filtered_DF['mean_dist']

            # pd.set_option('display.max_columns', None)
            #print(filtered_DF.head(20))

            # '''
            print("PROVA FDF CICLO ANOMALY SCORE")
            print(x)
            print(filtered_DF)
            # '''
            # -------------------- IQR calculation -----------------#

            Q3, Q1 = np.percentile(filtered_DF['anomaly_score'], [75, 25])
            # print(Q3)
            # print(Q1)
            IQR = Q3 - Q1
            # print(IQR)

            minimumThreshold = Q1 - 1.5 * IQR
            # print(minimumThreshold)
            print('minimum threshold = ' + str(minimumThreshold))

            maximumThreshold = Q3 + 1.5 * IQR
            print('maximum threshold = ' + str(maximumThreshold))

            # ---------------- Anomaly filtration by using IQR values --------------------------#

            minThreshold = minimumThreshold
            maxThreshold = maximumThreshold
            #filtered_anomaly = filtered_DF[
            #    (filtered_DF['anomaly_score'] > maxThreshold) | (filtered_DF['anomaly_score'] < minThreshold)]

            filtered_anomaly = filtered_DF[
                (filtered_DF['anomaly_score'] > maxThreshold) | (filtered_DF['anomaly_score'] < minThreshold)]

            if not filtered_anomaly.empty:
                filtered_anomalies = filtered_anomaly.copy()
            else:
                filtered_anomalies = filtered_DF.copy()
            #'''
            print("PROVA FA CLICLO")
            print(x)
            print(filtered_anomaly)
            #'''


            #print(filtered_anomaly.shape[0])
        #print("PROVA")
        #print(filtered_anomaly)

        #Removing columns used for calculations
        #filtered_anomaly.drop(columns=['mean_dist', 'cluster', 'dayORnight', 'sqr_dist', 'anomaly_score'], inplace=True)
        filtered_anomalies.drop(columns=['mean_dist', 'cluster', 'dayORnight', 'sqr_dist', 'anomaly_score'], inplace=True)

        # Added join from app to filtered_anomaly in order to display also PODID
        #filtered_anomaly = filtered_anomaly.join(app)
        filtered_anomalies = filtered_anomalies.join(app)

        #PRINT
        #print("KMEANS")
        #print(filtered_anomaly)
        return filtered_anomalies
    #print('Elapsed time is %f seconds.' % (time.time() - begin_time))
