class Accuracy:

    def __init__(self, dataengine, path_to_grand_truth, dataset, spark_session):
        self.dataengine = dataengine
        self.dataset = dataset
        self.path_to_grand_truth = path_to_grand_truth
        self.spark_session = spark_session

    def accuracy_calculation(self, flattening=1):
        final = self.dataengine.get_table_to_dataframe("Inferred_values", self.dataset).select(
            "tid", "attr_name", "attr_val")
        init = self.dataengine.get_table_to_dataframe("Observed_Possible_values_dk", self.dataset).select(
            "tid", "attr_name", "attr_val"
        )

        self.read()
        if flattening:
            self._flatting()

        incorrect = final.subtract(self.ground_truth_flat)
        errors = init.subtract(self.ground_truth_flat)
        corrected = errors.intersect(incorrect)
        incorrect_values = incorrect.count()
        repair = final.count()

        precision = float((repair - incorrect_values)) / repair
        recall = 1.0 - (float(corrected.count()) / errors.count())
        print ("The precision that we have is :" + str(precision))
        print ("The recall that we have is :" + str(recall))

    def _flatting(self):

        table_rv_attr_string = self.dataengine.get_schema(
            self.dataset, "Correct")
        rv_attrs = table_rv_attr_string.split(',')
        print rv_attrs

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
                               "( " + query_for_flattening + ");"
                counter += 1

                self.dataengine.query(insert_query)
        self.ground_truth_flat = self.dataengine.get_table_to_dataframe('Correct_flat', self.dataset)
        return

    def read(self):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file and the spark_session
        """
        self.ground_truth_flat = self.spark_session.read.csv(self.path_to_grand_truth, header=True)
        self.dataengine.add_db_table(
            'Correct', self.ground_truth_flat, self.dataset)
