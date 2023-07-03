from pyspark.sql import *
import pyspark.sql.functions as psf
import pandas as pd
from pathlib import Path
from interfaces.data_interface import DataInterface


class DynamicDataManager(DataInterface):
    '''
    def __init__(self, path='..\PELL_Data', podid=None, epid=None, start_t=None, end_t=None):
        self.path = Path(path)
        if self.path.is_absolute():
            self.path = self.path.as_uri()
        else:
            self.path = Path.cwd().joinpath(path).as_uri()
        self.podid = podid
        self.epid = epid
        self.start_t = start_t
        self.end_t = end_t

    # '''


    def load_jsons(self, path='..\PELL_Data', podid=None, epid=None, towncode=None, start_t=None, end_t=None):
        self.path= path
        self.podid = podid
        self.epid = epid
        self.towncode = towncode
        self.start_t = start_t
        self.end_t = end_t

        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("AnomalyDetection") \
            .getOrCreate()

            #------>    DBSCANAnomalyDetection???

        #----------DATA TRANSFORMATION---------
        dataFrameJSON = spark.read \
            .option("timestampFormat", "yyyy/MM/dd HH:mm:ss") \
            .json(self.path)              #Selection of the json file path given as input


        #USING PYSPARK TO ACCESS ENEA REMOTE MACHINE
        #dataFrameJSON = spark.read.format('json').option("timestampFormat", "yyyy/MM/dd HH:mm:ss").load("hdfs://192.168.34.109:54310/user/hdp/Data/pell-ip/pell-ip_menowattge_*")


        df1 = dataFrameJSON.select(
            dataFrameJSON["UrbanDataset"]["specification"]["name"].alias("name"),
            # dataFrameJSON["UrbanDataset"]["context"]["timestamp"].alias("timestamp"),
            psf.explode(dataFrameJSON["UrbanDataset"]["values"]["line"]).alias("data"))

        df2 = df1.select(
            # df1["timestamp"],
            df1["data"]["period"]["start_ts"].alias("start_time"),
            df1["data"]["period"]["end_ts"].alias("end_time"),
            df1["data"]["id"].alias("id"),
            psf.map_from_entries(df1["data"]["property"]).alias("prop"))

        # Changed Label name for Genova dataset
        # ActiveEnergy --> TotalActiveEnergy
        # ActivePowerPhase1 --> Phase1ActivePower
        # ActivePowerPhase2 --> Phase2ActivePower
        # ActivePowerPhase3 --> Phase3ActivePower


        df3 = df2.withColumn("ActiveEnergy", df2["prop"]["TotalActiveEnergy"].cast("double")) \
            .withColumn("Phase1ActivePower", df2["prop"]["Phase1ActivePower"].cast("double")) \
            .withColumn("Phase2ActivePower", df2["prop"]["Phase2ActivePower"].cast("double")) \
            .withColumn("Phase3ActivePower", df2["prop"]["Phase3ActivePower"].cast("double")) \
            .withColumn("PODID", df2["prop"]["PODID"].cast("string")) \
            .withColumn("EPID", df2["prop"]["ElectricPanelID"].cast("string")) \
            .withColumn("TownCode", df2["prop"]["TownCode"].cast("string")) \
            .drop(df2["prop"])
            #.withColumn("ID", df2["prop"]["PODID"]) \



        # df3.printSchema()
        # df3.show(5, truncate=False)
        # print(df3.count())

        #--------FEATURES SELECTION-----------
        panda_DF = df3.select("start_time", "end_time", "PODID", "EPID", "ActiveEnergy", "TownCode", "id").toPandas()

        #Selection based od start_time and end_time given as input
        panda_DF['start_time'] = pd.to_datetime(panda_DF['start_time'], format='%Y-%m-%d %H:%M:%S')
        panda_DF['end_time'] = pd.to_datetime(panda_DF['end_time'], format='%Y-%m-%d %H:%M:%S')

        if self.start_t and self.end_t:
           panda_DF = panda_DF[(panda_DF['start_time'] >= self.start_t) & (panda_DF['end_time'] <= self.end_t)]


        panda_DF['dayORnight'] = panda_DF['start_time'].apply(
            lambda x: '0' if int(x.strftime('%H')) > 20 or int(x.strftime('%H')) < 6 else '1')
        # panda_DF['Time'] = panda_DF['Time'].astype(str).astype(int)
        panda_DF.set_index(['start_time','id'], drop=True, inplace=True)


        #Selection based on the PODID and EPID given as input
        if self.podid:
            panda_DF = panda_DF[panda_DF['PODID'] == self.podid]
            if self.epid:
                panda_DF = panda_DF[panda_DF['EPID'] == self.epid]

        # Selection based on the TownCode given as input
        if self.towncode:
            panda_DF = panda_DF[panda_DF['TownCode'] == self.towncode]

        # Gestione dei valori mancanti di EPID
        panda_DF['EPID'].fillna('NA', inplace=True)
        panda_DF['TownCode'].fillna('NA', inplace=True)
        #count = panda_DF.shape[0]


        #print("VALORE FILTRATO")
        #print(count)

        #panda_DF.sort_index(inplace=True)

        #subtract all the data of ActiveEnergy
        '''
        amount_to_subtract = 0.0 #panda_DF['ActiveEnergy'].iloc[0]
        for i in range(count):
            print(i)
            print(count)
            panda_DF.loc[i, 'ActiveEnergy'] = panda_DF['ActiveEnergy'].iloc[i] + 0
            amount_to_subtract += panda_DF['ActiveEnergy'].iloc[i] #panda_DF.loc[i, 'ActiveEnergy']
        #'''
        #panda_DF['ActiveEnergy'] = panda_DF['ActiveEnergy'] - amount_to_subtract
        #print('AMOUNT TO SUBTRACT: ')
        #print(amount_to_subtract)

        panda_DF = panda_DF.sort_index(level='id', ascending=False)

        panda_DF = panda_DF[~panda_DF.index.get_level_values('start_time').duplicated(keep='first')]

        #panda_DF = panda_DF.sort_values(by='id', ascending=False)
        #panda_DF = panda_DF.drop_duplicates(subset='start_time', keep='first')

        panda_DF.sort_index(inplace=True)

        panda_DF.reset_index(inplace=True)
        panda_DF.drop(columns=["id"], inplace=True)
        panda_DF.set_index(['start_time'], drop=True, inplace=True)

        # METTO PRINT
        print('PELL STREET LIGHTNING')
        print(panda_DF)

        return panda_DF

    def load_PODID(self):
        df_podid = self.load_jsons()
        df_podid.drop(columns=["EPID", "end_time", "ActiveEnergy", "dayORnight", "TownCode"], inplace=True)
        df_podid.reset_index(drop=True, inplace=True)
        df_podid.drop_duplicates(inplace=True)
        # METTO PRINT
        print('LOAD PODID')
        print(df_podid)

        return df_podid.reset_index()

    def load_ActiveEnergy(self, podid):
        self.podid = podid

        df_AE = self.load_jsons()
        df_AE.reset_index(inplace=True)
        df_AE.drop(columns=["id", "PODID", "EPID", "end_time", "dayORnight", "TownCode"], inplace=True)
        df_AE.set_index(['start_time'], drop=True, inplace=True)


        # subtract all the data of ActiveEnergy
        '''
        print('LOAD ACTIVE ENERGY')
        print(df_AE)
        
        count = df_AE.shape[0]
        amount_to_subtract = 0
        for i in range(count):
            print(i)
            #print(count)

            print('PRIMA')
            print(df_AE['ActiveEnergy'].iloc[i])
            print(amount_to_subtract)

            before = df_AE['ActiveEnergy'].iloc[i]
            df_AE.loc[i, 'ActiveEnergy'] = df_AE['ActiveEnergy'].iloc[i] - amount_to_subtract
            amount_to_subtract = before

            print('DOPO')
            print(df_AE['ActiveEnergy'].iloc[i])
            print(amount_to_subtract)

        #'''
        # METTO PRINT
        print('LOAD ACTIVE ENERGY')
        print(df_AE)

        return df_AE

