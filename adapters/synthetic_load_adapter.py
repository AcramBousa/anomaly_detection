import random


class SyntheticAnomalies:

    '''
    def __init__(self, scenario, path='PELL_Data', podid=None, epid=None, start_t=None, end_t=None):
        self.data_mgr = DynamicDataManager(path, podid, epid, start_t, end_t)
        self.df = self.data_mgr.load_jsons()
        self.scenario = scenario
    #'''

    def synthetic_anomalies(self, dataframe, scenario):
        self.df = dataframe
        self.scenario = scenario
        self.list_of_anomalies = []

        if self.scenario == 1:
            return self.synthetic_anomalies_S1()
        elif self.scenario == 2:
            return self.synthetic_anomalies_S2()
        elif self.scenario == 3:
            return self.synthetic_anomalies_S3()
        elif self.scenario == 4:
            return self.synthetic_anomalies_S4()
        elif self.scenario == 5:
            return self.synthetic_anomalies_S5()
        elif self.scenario == 6:
            return self.synthetic_anomalies_S6()
        else:
            return self.df

    # S1: To handle random positive high peaks at the night time. For example, a
    # terminal operating at the night time and some random high peaks encountered, which deviates
    # from the normal consumption measures.
    def synthetic_anomalies_S1(self):
    
        # For night, filter the data frame
        updated_df = self.df[self.df['dayORnight'] == '0']

        min_range = 118000                                #15000           #300      #118000 PER GENOVA
        max_range = 130000                                #20000           #350      #130000
        self.number_of_anomalies = 100                    #2000            #40       #100 PER GENOVA

        for _ in range(self.number_of_anomalies):
            self.list_of_anomalies.append(random.randint(min_range, max_range))

        #print("LIST OF ANOMALIES")
        #print(self.list_of_anomalies) #COMMENTATO

        self.random_rows = updated_df.sample(self.number_of_anomalies, replace=False).index
        #print("RANDOM ROWS")
        #print(self.random_rows) #COMMENTATO

        #print(updated_df)

        for i in range(self.number_of_anomalies):
            updated_df.loc[self.random_rows[i], 'ActiveEnergy'] = self.list_of_anomalies[i]

        return updated_df



    # S2: To handle random negative high peaks at the day time. For example, a
    # terminal is switched off during the day time and some random negative peaks encountered.
    def synthetic_anomalies_S2(self):

        # For day, filter the data frame
        updated_df = self.df[self.df['dayORnight'] == '1']

        min_range = -50
        max_range = -30
        self.number_of_anomalies = 5

        self.list_of_anomalies = []

        for _ in range(self.number_of_anomalies):
            self.list_of_anomalies.append(random.randint(min_range, max_range))
        print(self.list_of_anomalies)

        self.random_rows = updated_df.sample(self.number_of_anomalies, replace=False).index
        print(self.random_rows)
        #updated_df.loc[random_rows, 'ActiveEnergy'] = list_of_anomalies

        for i in range(self.number_of_anomalies):
            updated_df.loc[self.random_rows[i], 'ActiveEnergy'] = self.list_of_anomalies[i]
        return updated_df

    # S3: To handle random low peaks at the night time. For example, a terminal is
    # operating at the night and some random low peaks encountered, which are different from the
    # normal consumption measures.
    def synthetic_anomalies_S3(self):

        # For night, filter the data frame
        updated_df = self.df[self.df['dayORnight'] == '0']

        min_range = 0.50
        max_range = 2.90
        self.number_of_anomalies = 5

        self.list_of_anomalies = []

        for _ in range(self.number_of_anomalies):
            self.list_of_anomalies.append(round(random.uniform(min_range, max_range), 2))
        print(self.list_of_anomalies)

        self.random_rows = updated_df.sample(self.number_of_anomalies, replace=False).index
        print(self.random_rows)

        for i in range(self.number_of_anomalies):
            updated_df.loc[self.random_rows[i], 'ActiveEnergy'] = self.list_of_anomalies[i]

        return updated_df

    # S5: To handle random high peaks at the day time. For example, a terminal was
    # switched off at the day time and some random high peaks encountered, which deviates from
    # the normal consumption measures at the day time.
    def synthetic_anomalies_S5(self):

        # For day, filter the data frame
        updated_df = self.df[self.df['dayORnight'] == '1']

        min_range = 250
        max_range = 290
        self.number_of_anomalies = 5

        self.list_of_anomalies = []

        for _ in range(self.number_of_anomalies):
            self.list_of_anomalies.append(random.randint(min_range, max_range))
        print(self.list_of_anomalies)

        self.random_rows = updated_df.sample(self.number_of_anomalies, replace=False).index
        print(self.random_rows)

        for i in range(self.number_of_anomalies):
            updated_df.loc[self.random_rows[i], 'ActiveEnergy'] = self.list_of_anomalies[i]

        return updated_df

    # S4: To handle continuous low peaks at the night. For example, a terminal is
    # switched off for a duration of three to four hours at the night time.
    def synthetic_anomalies_S4(self):

        # For night, filter the data frame
        updated_df = self.df[self.df['dayORnight'] == '0']

        min_range = 0.50
        max_range = 2.90
        self.number_of_anomalies = 10

        self.list_of_anomalies = []

        for _ in range(self.number_of_anomalies):
            self.list_of_anomalies.append(round(random.uniform(min_range, max_range), 2))
        print(self.list_of_anomalies)

        n = random.randint(0, len(updated_df) - self.number_of_anomalies)
        self.random_consecutive_rows = updated_df[n:n + self.number_of_anomalies].index
        print(self.random_consecutive_rows)

        for i in range(self.number_of_anomalies):
            updated_df.loc[self.random_consecutive_rows[i], 'ActiveEnergy'] = self.list_of_anomalies[i]

        return updated_df

    # S6: To handle continuous high peaks at the day time. For example, a terminal
    # switched on for a duration of three to four hours at day time.
    def synthetic_anomalies_S6(self):

        # For day, filter the data frame
        updated_df = self.df[self.df['dayORnight'] == '1']

        min_range = 180
        max_range = 200
        self.number_of_anomalies = 10

        self.list_of_anomalies = []

        for _ in range(self.number_of_anomalies):
            self.list_of_anomalies.append(random.randint(min_range, max_range))
        print(self.list_of_anomalies)

        n = random.randint(0, len(updated_df) - self.number_of_anomalies)
        self.random_consecutive_rows = updated_df[n:n + self.number_of_anomalies].index
        print(self.random_consecutive_rows)

        for i in range(self.number_of_anomalies):
            updated_df.loc[self.random_consecutive_rows[i], 'ActiveEnergy'] = self.list_of_anomalies[i]

        return updated_df

