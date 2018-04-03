from holoclean.global_variables import GlobalVariables
from holoclean.utils.reader import Reader


class Accuracy:

    def __init__(
            self,
            session,
            path_to_grand_truth
    ):
        self.dataengine = session.holo_env.dataengine
        self.dataset = session.dataset
        self.path_to_grand_truth = path_to_grand_truth
        self.spark_session = session.holo_env.spark_session
        self.ground_truth_flat = None
        self.session = session

    def accuracy_calculation(self, flattening=1):


        inferred = None

        try:
            inferred_multivalues  = self.dataengine.get_table_to_dataframe(
                "Inferred_values", self.dataset).select(
                "tid", "attr_name", "attr_val")

        except:
            self.session.holo_env.logger.error('No Inferred values')
            inferred_multivalues = None

        try:
            inferred_singlevalues= \
                self.session.simple_predictions.select("tid", "attr_name", "attr_val")
        except:
            self.session.holo_env.logger.error('No Simple values ')
            inferred_singlevalues = None

        if inferred_multivalues and inferred_singlevalues :
            inferred = inferred_multivalues.union (inferred_singlevalues)

        elif inferred_singlevalues:
            inferred = inferred_singlevalues

        elif inferred_multivalues:
            inferred = inferred_multivalues
        else:
            print ("The precision and recall cannot be calculated")
            return


        init = self.dataengine.get_table_to_dataframe(
            "Observed_Possible_values_dk", self.dataset).select(
            "tid", "attr_name", "attr_val")

        self.read()
        if flattening:
            self._flatting()


        if inferred_multivalues:
            inferred_multivalues_count = inferred_multivalues.count()
            incorrect_multivalues = inferred_multivalues.subtract(self.ground_truth_flat)
            incorrect_multivalues_count = incorrect_multivalues.count()
            original_multivalues_errors = init.subtract(inferred_singlevalues).subtract(self.ground_truth_flat)
            original_multivalues_errors_count = original_multivalues_errors.count()
            uncorrected_multivalues = original_multivalues_errors.drop('attr_val').intersect(
                incorrect_multivalues.drop('attr_val'))
            uncorrected_multivalues_count = uncorrected_multivalues.count()

            multivalues_recall = 1.0 - (float(uncorrected_multivalues_count) / original_multivalues_errors_count)
            multivalues_precision = float(
            inferred_multivalues_count - incorrect_multivalues_count) / inferred_multivalues_count
            print ("The multiple-values precision that we have is :" + str(multivalues_precision))
            print ("The multiple-values recall that we have is :" + str(multivalues_recall) + "out of " + str(
                original_multivalues_errors_count))


        if inferred_singlevalues:
            inferred_singlevalues_count = inferred_singlevalues.count()
            incorrect_singlevalues = inferred_singlevalues.subtract(self.ground_truth_flat)
            incorrect_singlevalues_count = incorrect_singlevalues.count()
            original_singlevalues_errors = inferred_singlevalues.subtract(self.ground_truth_flat)
            original_singlevalues_errors_count = original_singlevalues_errors.count()
            uncorrected_singlevalues = original_singlevalues_errors.drop('attr_val').intersect(
                incorrect_singlevalues.drop('attr_val'))
            uncorrected_singlevalues_count = uncorrected_singlevalues.count()

            singlevalues_recall = 1.0 - (float(uncorrected_singlevalues_count) / original_singlevalues_errors_count)
            singlevalues_precision = float(
                inferred_singlevalues_count - incorrect_singlevalues_count) / inferred_singlevalues_count
            print ("The single-value precision that we have is :" + str(singlevalues_precision))
            print ("The single-value recall that we have is :" + str(singlevalues_recall) + "out of " + str(
                original_singlevalues_errors_count))



        inferred_count = inferred.count()
        incorrect_inferred = inferred.subtract(self.ground_truth_flat)
        incorrect_count = incorrect_inferred.count()
        original_errors = init.subtract(self.ground_truth_flat)
        original_errors_count = original_errors.count()
        uncorrected = original_errors.drop('attr_val').intersect(incorrect_inferred.drop('attr_val'))
        uncorrected_count = uncorrected.count()

        recall = 1.0 - (float(uncorrected_count) / original_errors_count)
        precision = float((inferred_count - incorrect_count)) / inferred_count
        print ("The precision that we have is :" + str(precision))
        print ("The recall that we have is :" + str(recall) + "out of " + str(original_errors_count) )




    def _flatting(self):

        table_rv_attr_string = self.dataset.schema.remove(GlobalVariables.index_name)
        rv_attrs = table_rv_attr_string.split(',')
        print rv_attrs

        query_for_flattening = "CREATE TABLE \
                    " + self.dataset.table_specific_name('Correct_flat') \
                               + "( rv_index TEXT, \
                    rv_attr TEXT, attr_val TEXT);"
        self.dataengine.query(query_for_flattening)

        counter = 0

        for rv_attr in rv_attrs:
            if rv_attr != GlobalVariables.index_name:
                query_for_flattening = """ (SELECT \
                                       DISTINCT t1.__ind as tid,'""" + \
                                       rv_attr + """'  \
                                       AS attr_name, \
                                       t1.""" + rv_attr + """ AS attr_val \
                                       FROM """ + \
                                       self.dataset.\
                                       table_specific_name('Correct') +\
                                       " as t1)"

                insert_query = "INSERT INTO " + self.dataset.\
                    table_specific_name('Correct_flat') + \
                               "( " + query_for_flattening + ");"
                counter += 1

                self.dataengine.query(insert_query)
        self.ground_truth_flat = self.dataengine.get_table_to_dataframe(
            'Correct_flat', self.dataset)
        return

    def read(self):
        """Create a dataframe from the csv file

        Takes as argument the full path name of the csv file
        and the spark_session
        """
        filereader = Reader(self.spark_session)
        self.ground_truth_flat = filereader.read(self.path_to_grand_truth).drop(GlobalVariables.index_name)
        self.dataengine.add_db_table(
            'Correct', self.ground_truth_flat, self.dataset)
