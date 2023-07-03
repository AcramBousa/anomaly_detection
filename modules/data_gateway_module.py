class DataGateway:
    def __init__(self, data_loader):
        self.data_loader = data_loader

    def load_data(self, path = 'PELL_Data/Genova', podid=None, epid=None, start_t=None, end_t=None):
        dataframe = self.data_loader.load_jsons(path = path, podid = podid, epid = epid, start_t = start_t, end_t = end_t)
        return dataframe