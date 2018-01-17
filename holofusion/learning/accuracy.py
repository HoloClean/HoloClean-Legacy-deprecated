class Accuracy:

    def __init__(self, dataengine, path_to_ground_truth, dataset, spark_session):
        self.dataengine = dataengine
        self.dataset = dataset
        self.path_to_ground_truth = path_to_ground_truth
        self.spark_session = spark_session
        self.key = ""

    def book_accuracy(self):
        rv_attr = "Dept_Gate"
        final = self.dataengine.get_table_to_dataframe("Final", self.dataset)
        final_authors = final.filter(final.rv_attr == rv_attr)
        print("show only authors")
        final_authors.show()
        final_authors_list=final_authors.collect()
        f = open('results/fusion.txt', 'w')
        for ele in final_authors_list:
            f.write(str(ele) + '\n')
        f.close()
        #final_authors.write.format("com.databricks.spark.csv").option("header", "true").save("file1.csv")
        print ("show ground_truth")
        ground_truth_specific = self.ground_truth_flat.filter(self.ground_truth_flat.rv_attr == rv_attr)
        ground_truth_specific.show()
        ground_truth_list=ground_truth_specific.collect()
        f = open('results/ground_truth.txt', 'w')
        for ele in ground_truth_list:
            f.write(str(ele) + '\n')

        f.close()
        #self.ground_truth_flat.write.format("com.databricks.spark.csv").option("header", "true").save("file.csv")
        incorrect = ground_truth_specific.subtract(final_authors)
        print("show incorrect values")
        incorrect.show()
        incorrect_list=incorrect.collect()
        f = open('results/incorrect.txt', 'w')
        for ele in incorrect_list:
            f.write(str(ele) + '\n')

        f.close()
        #incorrect.write.format("com.databricks.spark.csv").option("header", "true").save("file3.csv")
        all_values = ground_truth_specific.count()
        print ("the number of tuples for the ground truth is :")
        print (all_values)
        incorrect_values = incorrect.count()
        print("the incorrect values are:")
        print(incorrect_values)
        print ("the accuracy is:")
        accuracy=(1.0)*(all_values-incorrect_values)/all_values
        print accuracy


    def read(self):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file and the spark_session
        """
        self.ground_truth = self.spark_session.read.csv(self.path_to_ground_truth, header=True)
        self.dataengine.add_db_table('Correct', self.ground_truth, self.dataset)
        return

    def flatting(self):

        table_rv_attr_string = self.dataengine.get_schema(
            self.dataset, "Correct")
        rv_attrs = table_rv_attr_string.split(',')
        print rv_attrs
        self.key = 'Flight_Num'
        #self.key = raw_input("give the rv_attr that distinguis the objects:")

        while self.key not in rv_attrs:
            self.key = raw_input("give the rv_attr that distinguis the objects:")
        table_rv_attr_string = self.dataengine.get_schema(
            self.dataset, "Correct")
        rv_attrs = table_rv_attr_string.split(',')
        counter = 0

        query_for_featurization = "CREATE TABLE \
                    " + self.dataset.table_specific_name('Correct_flat') \
                                  + "( rv_index TEXT, \
                    rv_attr TEXT, assigned_val TEXT);"
        self.dataengine.query(query_for_featurization)

        insert_signal_query = ""
        for rv_attr in rv_attrs:
            if rv_attr != self.key and rv_attr != "Source" and rv_attr != "Index":
                query_for_featurization = """ (SELECT \
                                                  init.""" + self.key + """ as rv_index,'""" + rv_attr + """'  \
                                                  AS rv_attr, \
                                                  init.""" + rv_attr + """ AS assigned_val \
                                                  FROM """ + \
                                          self.dataset.table_specific_name('Correct') + \
                                          " AS init )"
                insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name('Correct_flat') + \
                                      " SELECT * FROM ( " + query_for_featurization + \
                                      "as T_" + str(counter) + ");"
                counter += 1
                print insert_signal_query
                self.dataengine.query(insert_signal_query)

        self.ground_truth_flat = self.dataengine.get_table_to_dataframe('Correct_flat', self.dataset)
