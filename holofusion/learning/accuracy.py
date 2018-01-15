class Accuracy:

    def __init__(self, dataengine, path_to_grand_truth, dataset, spark_session):
        self.dataengine = dataengine
        self.dataset = dataset
        self.path_to_grand_truth = path_to_grand_truth
        self.spark_session = spark_session
        self.key = ""

    def book_accuracy(self):
        final = self.dataengine.get_table_to_dataframe("Final", self.dataset)
        final_authors = final.filter(final.attribute == "Author_list")
        print("show only authors")
        final_authors.show()
        final_authors.write.format("com.databricks.spark.csv").option("header", "true").save("file1.csv")
        print ("show ground_truth")
        self.grand_truth_flat.show()
        self.grand_truth_flat.write.format("com.databricks.spark.csv").option("header", "true").save("file.csv")
        incorrect = self.grand_truth_flat.subtract(final_authors)
        print("show incorrect values")
        incorrect.show()
        incorrect.write.format("com.databricks.spark.csv").option("header", "true").save("file3.csv")
        all_values = self.grand_truth_flat.count()
        print ("the number of tuples for the ground truth is :")
        print (all_values)
        incorrect_values = incorrect.count()
        print("the incorrect values are:")
        print(incorrect_values)
        print ("the accuracy is:")
        accuracy=(1.0)*(all_values-incorrect_values)/all_values
        print accuracy
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

        self.grand_truth_flat = self.dataengine.get_table_to_dataframe('Correct_flat', self.dataset)
