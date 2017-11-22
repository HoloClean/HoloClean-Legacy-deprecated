import holoclean.holoclean

class Accuracy:
    def __init__(self, path_to_grand_truth, dataset, spark_session):
        self.dataset=dataset
        self.path_to_grand_truth=path_to_grand_truth
        self.spark_session=spark_session
        pass

    def ingest(self):
        pass

    def accuracy_claculation(self):

        pass

    def read(self):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file and the spark_session
        """
        df = self.spark_session.read.csv(self.path_to_grand_truth, header=True)
        return df