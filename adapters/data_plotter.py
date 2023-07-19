import pandas as pd
from interfaces.data_plot_interface import DataPlotterInterface
import matplotlib.pyplot as plt
from os import path

class DataPlotter(DataPlotterInterface):

    def plot_data(self, dataframe, message = ''):
        dataframe.reset_index(inplace=True)
        dataframe.set_index(['start_time'], drop=True, inplace=True)
        podid = dataframe["PODID"].iloc[0]

        self.dataframe = dataframe["ActiveEnergy"]

        self.dataframe = pd.DataFrame(self.dataframe)
        #self.dataframe.plot(title=podid, xlabel="TIME", ylabel="ACTIVE ENERGY")
        #'''
        self.dataframe.reset_index(drop=False, inplace=True)
        self.dataframe.rename(columns={"index": "start_time"}, inplace=True)
        self.dataframe.plot(x = 'start_time', y="ActiveEnergy", title=podid, xlabel="TIME", ylabel="ACTIVE ENERGY",
                            kind='scatter')
        #'''
        if not message == '':
            message =  message + '_'
        plt.savefig(path.join('PELL_Plots/', f'{podid}_Plot_{message}Anomalies.png'))#_{message}Anomalies