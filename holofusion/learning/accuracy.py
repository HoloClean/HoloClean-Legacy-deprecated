class Accuracy:

    def __init__(self, dataengine, path_to_grand_truth, dataset, spark_session):
        self.dataengine = dataengine
        self.dataset = dataset
        self.path_to_grand_truth = path_to_grand_truth
        self.spark_session = spark_session
        self.key=""

    def accuracy_calculation(self):
        # precision=0
        dont_know_cells_df = self.dataengine.get_table_to_dataframe("C_dk", self.dataset)
        tmp = dont_know_cells_df.collect()

        dont_know_cells_ind = [list_count.asDict()['ind'] for list_count in tmp]
        dont_know_cells_attr = [list_count.asDict()['attr'] for list_count in tmp]
        self.read()
        value = []
        for cell in range(len(dont_know_cells_ind)):
            tmp_row = self.grand_truth.select(dont_know_cells_attr[cell]).filter(
                self.grand_truth.index == dont_know_cells_ind[cell]).collect()
            # print tmp_row
            value.append(tmp_row[0].asDict()[dont_know_cells_attr[cell]])
        grad_truth_dk = zip(dont_know_cells_ind, dont_know_cells_attr, value)
        # print grad_truth_dk
        schema_grand_truth = ["rv_index", "rv_attr", "assigned_val"]
        grand_truth_dk_cells_df = self.spark_session.createDataFrame(grad_truth_dk, schema_grand_truth)
        number_of_repairs = dont_know_cells_df.count()
        repair_value = self.dataengine.get_table_to_dataframe("Final", self.dataset)
        # repair_value.show()
        # grand_truth_dk_cells_df.show()
        incorrect_repairs = repair_value.subtract(grand_truth_dk_cells_df).count()
        correct_repairs = grand_truth_dk_cells_df.count()

        # We find the precision, recall ,and F1 score
        precision = 1.0 * (number_of_repairs - incorrect_repairs) / number_of_repairs
        recall = 1.0 * (number_of_repairs - incorrect_repairs) / correct_repairs
        f1_score = 2.0 * (precision * recall) / (precision + recall)
        # print number_of_repairs
        # print incorrect_repairs

        print ("The precision that we have is :" + str(precision))
        print ("The recall that we have is :" + str(recall))
        print ("The F1 score that we have is :" + str(f1_score))
        
    def hospital_accuracy(self):
        final = self.dataengine.get_table_to_dataframe("Final", self.dataset)
        incorrect = final.substact(self.grand_truth)
        incorrect_values = incorrect.count()
        repair = final.count()
        all_corect = self.grand_truth.count()

        precision = float((repair - incorrect_values)) / repair
        recall = (repair - incorrect_values) / all_corect
        f1_score = 2.0 * (precision * recall) / (precision + recall)
        print ("The precision that we have is :" + str(precision))
        print ("The recall that we have is :" + str(recall))
        print ("The F1 score that we have is :" + str(f1_score))

    def book_accuracy(self):
        final = self.dataengine.get_table_to_dataframe("Final", self.dataset)
        final.show()
        self.grand_truth2.show()
        incorrect = final.subtract(self.grand_truth2)
        incorrect.show()
        incorrect_values = incorrect.count()
        repair = final.count()
        return

    def read(self):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file and the spark_session
        """
        self.grand_truth = self.spark_session.read.csv(self.path_to_grand_truth, header=True)
        self.dataengine.add_db_table('Correct', self.grand_truth, self.dataset)
        self.grand_truth1 = self.grand_truth.drop('Index')
        return

    def flatting(self):

        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Correct")
        attributes = table_attribute_string.split(',')
        print attributes
        self.key = 'ISBN'
        #self.key = raw_input("give the attribute that distinguis the objects:")

        while self.key not in attributes:
            self.key = raw_input("give the attribute that distinguis the objects:")
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Correct")
        attributes = table_attribute_string.split(',')
        counter = 0

        query_for_featurization = "CREATE TABLE \
                    " + self.dataset.table_specific_name('Correct_flat') \
                                  + "( key_id TEXT, \
                    attribute TEXT, source_observation TEXT);"
        self.dataengine.query(query_for_featurization)

        insert_signal_query = ""
        for attribute in attributes:
            if attribute != self.key and attribute != "Source" and attribute != "Index":
                query_for_featurization = """ (SELECT \
                                                  init.""" + self.key + """ as key_id,'""" + attribute + """'  \
                                                  AS attribute, \
                                                  init.""" + attribute + """ AS source_observation \
                                                  FROM """ + \
                                          self.dataset.table_specific_name('Correct') + \
                                          " AS init )"
                insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name('Correct_flat') + \
                                      " SELECT * FROM ( " + query_for_featurization + \
                                      "as T_" + str(counter) + ");"
                counter += 1
                print insert_signal_query
                self.dataengine.query(insert_signal_query)

        self.grand_truth2=self.dataengine.get_table_to_dataframe('Correct_flat', self.dataset)