class Accuracy:

    def __init__(self, dataengine, path_to_grand_truth, dataset, spark_session):
        self.dataengine = dataengine
        self.dataset = dataset
        self.path_to_grand_truth = path_to_grand_truth
        self.spark_session = spark_session

    def accuracy_calculation(self, flattening = 1):
        # precision=0
        final = self.dataengine.get_table_to_dataframe("Inferred_values", self.dataset).select(
            "tid", "attr_name", "attr_val")
        init = self.dataengine.get_table_to_dataframe("Observed_Possible_values_dk", self.dataset).select(
            "tid", "attr_name", "attr_val"
        )
        '''init_dk = self.dataengine.get_table_to_dataframe("Observed_Possible_values_clean", self.dataset).select(
            "tid", "attr_name", "attr_val"
        )
        init = init_clean.union(init_dk)'''

        final.show()

        self.read()
        if flattening :
            self._flatting()
        self.ground_truth_flat.show()

        incorrect = final.subtract(self.ground_truth_flat)
        errors = init.subtract(self.ground_truth_flat)
        corrected = errors.intersect(incorrect)
        incorrect_values = incorrect.count()
        repair = final.count()

        all_corect = self.ground_truth_flat.count()

        print "debug:"
        print init.subtract(self.ground_truth_flat).count()
        print errors.intersect(incorrect).count()

        self.dataengine.add_db_table('Initial_Errors', errors, self.dataset)
        self.dataengine.add_db_table('Incorrect_Repairs', incorrect, self.dataset)
        precision = float((repair - incorrect_values)) / repair
        recall = 1.0 - (float(corrected.count()) / errors.count())
        #f1_score = 2.0 * (precision * recall) / (precision + recall)
        print ("The precision that we have is :" + str(precision))
        print ("The recall that we have is :" + str(recall))
        #print ("The F1 score that we have is :" + str(f1_score))

        '''
        print("show incorrect values")
        incorrect.show()


        all_values = dont_know_cells_df.count()
        print ("the number of tuples on the DK is :")
        print (all_values)
        incorrect_values = incorrect.count()
        print("the incorrect values are:")
        print(incorrect_values)
        print ("the accuracy is:")
        accuracy = (1.0)*(all_values-incorrect_values)/all_values
        print accuracy
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
        print ("The F1 score that we have is :" + str(f1_score))'''
        return

    def _flatting(self):

        table_rv_attr_string = self.dataengine.get_schema(
            self.dataset, "Correct")
        rv_attrs = table_rv_attr_string.split(',')
        print rv_attrs
        # self.key = raw_input("give the rv_attr that distinguis the objects:")

        query_for_flattening = "CREATE TABLE \
                    " + self.dataset.table_specific_name('Correct_flat') \
                               + "( rv_index TEXT, \
                    rv_attr TEXT, attr_val TEXT);"
        self.dataengine.query(query_for_flattening)

        counter = 0

        for rv_attr in rv_attrs:
            if rv_attr != "index" and rv_attr != "Index":
                query_for_flattening = """ (SELECT \
                                                  DISTINCT table1.index as tid,'""" + \
                                       rv_attr + """'  \
                                                  AS attr_name, \
                                                  table1.""" + rv_attr + """ AS attr_val \
                                                  FROM """ + \
                                       self.dataset.table_specific_name('Correct') + " as table1)"

                insert_query = "INSERT INTO " + self.dataset.table_specific_name('Correct_flat') + \
                               " SELECT * FROM ( " + query_for_flattening + \
                               "as T_" + str(counter) + ");"
                counter += 1

                self.dataengine.query(insert_query)
        self.ground_truth_flat = self.dataengine.get_table_to_dataframe('Correct_flat', self.dataset)
        return

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

    def read(self):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file and the spark_session
        """
        self.ground_truth_flat = self.spark_session.read.csv(self.path_to_grand_truth, header=True)
        self.dataengine.add_db_table(
            'Correct', self.ground_truth_flat, self.dataset)
