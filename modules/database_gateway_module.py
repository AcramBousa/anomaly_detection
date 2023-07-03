class DatabaseGateway:
    def __init__(self, database_writer):
        self.database_writer = database_writer

    def write_to_database(self, anomalies):
        self.database_writer.write_to_database(anomalies)