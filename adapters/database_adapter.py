from interfaces.database_interface import DatabaseInterface
import pandas as pd
from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


class SQLDatabaseGateway(DatabaseInterface):
    def write_to_database(self, anomalies):
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("AnomalyDetection") \
            .getOrCreate()

        # Configurazione delle opzioni di scrittura per il database
        database_url = "jdbc:mysql://localhost:3306/anomalies"
        table_name = "detected_anomalies"
        username = "username"
        password = "password"

        anomalies = pd.DataFrame(anomalies)
        print("ANOMALIES PD")
        print(anomalies)
        #anomalies.reset_index(drop=False, inplace=True)

        #anomalies_spark = spark.createDataFrame

        anomalies = pd.DataFrame(anomalies)
        anomalies.reset_index(drop=False, inplace=True)
        anomalies.rename(columns={"index": "start_time"}, inplace=True)

        # Creazione del dataframe PySpark
        anomalies_spark = spark.createDataFrame(anomalies)

        print("ANOMALIES SPARK")
        anomalies_spark.show()

        # Scrittura del DataFrame nel database
        anomalies_spark.write.format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", database_url) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .mode("append") \
            .save()

        return anomalies