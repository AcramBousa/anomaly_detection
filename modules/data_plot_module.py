class DataPlotGateway:
    def __init__(self, data_plotter):
        self.data_plotter = data_plotter

    def plot_data(self, dataframe):
        self.data_plotter.plot_data(dataframe)