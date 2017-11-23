class Accuracy:
    def __init__(self, dataengine, path_to_grand_truth, dataset, spark_session):
        self.dataengine = dataengine
        self.dataset = dataset
        self.path_to_grand_truth = path_to_grand_truth
        self.spark_session = spark_session


    def accuracy_calculation(self):
        # precision=0
        dont_know_cells_df = self.dataengine._table_to_dataframe("C_dk", self.dataset)
        tmp=dont_know_cells_df.collect()

        dont_know_cells_ind=[list_count.asDict()['ind'] for list_count in tmp]
        dont_know_cells_attr=[list_count.asDict()['attr'] for list_count in tmp]
        self.read()
        value=[]
        for cell in range(len(dont_know_cells_ind)):
            tmp_row = self.grand_truth.select(dont_know_cells_attr[cell]).filter(self.grand_truth.index==dont_know_cells_ind[cell]).collect()
            # print tmp_row
            value.append(tmp_row[0].asDict()[dont_know_cells_attr[cell]])
        grad_truth_dk = zip(dont_know_cells_ind, dont_know_cells_attr, value)
        # print grad_truth_dk
        schema_grand_truth=["rv_index","rv_attr","assigned_val"]
        grand_truth_dk_cells_df=self.spark_session.createDataFrame(grad_truth_dk,schema_grand_truth)
        number_of_repairs = dont_know_cells_df.count()
        repair_value = self.dataengine._table_to_dataframe("Final", self.dataset)
        repair_value.show()
        grand_truth_dk_cells_df.show()
        incorrect_repairs = repair_value.subtract(grand_truth_dk_cells_df).count()
        correct_repairs=grand_truth_dk_cells_df.count()
        print number_of_repairs
        print incorrect_repairs
        raw_input("sad")

        # We find the precision, recall ,and F1 score
        precision = (1.0) * (number_of_repairs - incorrect_repairs) / number_of_repairs
        recall = (1.0) * (number_of_repairs - incorrect_repairs) / correct_repairs
        f1_score = (2.0) * (precision * recall) / (precision + recall)
        print number_of_repairs
        print incorrect_repairs

        print ("The precision that we have is :" + str(precision))
        print ("The recall that we have is :" + str(recall))
        print ("The F1 score that we have is :" + str(f1_score))

    def read(self):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file and the spark_session
        """
        self.grand_truth = self.spark_session.read.csv(self.path_to_grand_truth, header=True)
