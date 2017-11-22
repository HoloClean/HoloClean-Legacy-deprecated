import holoclean.holoclean

class Accuracy:
    def __init__(self, path_to_grand_truth, dataset, spark_session):
        self.dataset=dataset
        self.path_to_grand_truth=path_to_grand_truth
        self.spark_session=spark_session


    def accuracy_calculation(self):
	accuracy=0
	dataframe1 = self.dataengine._table_to_dataframe("C_dk", self.dataset)
        number_of_repairs = dataframe1.count()
	df1 = self.dataengine._table_to_dataframe("Changes", self.dataset)
        incorrect_repairs = df1.subtract(self.df).count()

        # We find the accuracy
        accuracy = (1.0) * (number_of_repairs - incorrect_repairs) / number_of_repairs



    def read(self):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file and the spark_session
        """
        self.df = self.spark_session.read.csv(self.path_to_grand_truth, header=True)
