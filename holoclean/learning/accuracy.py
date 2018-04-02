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

        inferred_multivalues  = self.dataengine.get_table_to_dataframe(
            "Inferred_values", self.dataset).select(
            "tid", "attr_name", "attr_val")

        inferred_singlevalues= self.session.simple_predictions.select(
            "tid", "attr_name", "attr_val")

        inferred = inferred_multivalues.union (inferred_singlevalues)

        init = self.dataengine.get_table_to_dataframe(
            "Observed_Possible_values_dk", self.dataset).select(
            "tid", "attr_name", "attr_val")

        self.read()
        if flattening:
            self._flatting()

        incorrect_multivalues = inferred_multivalues.subtract(self.ground_truth_flat)
        incorrect_singlevalues = inferred_singlevalues.subtract(self.ground_truth_flat)
        incorrect_inferred = inferred.subtract(self.ground_truth_flat)

        incorrect_count = incorrect_inferred.count()
        incorrect_multivalues_count = incorrect_multivalues.count()
        incorrect_singlevalues_count = incorrect_singlevalues.count()

        errors = init.subtract(self.ground_truth_flat)

        never_touched = errors.drop('attr_val').subtract(incorrect_inferred.drop('attr_val'))

        remaining_incorrect_multivalues = errors.drop('attr_val').intersect(incorrect_multivalues.drop('attr_val'))
        remaining_incorrect_singlevalues = errors.drop('attr_val').intersect(incorrect_singlevalues.drop('attr_val'))
        remaining_incorrect = errors.drop('attr_val').intersect(incorrect_inferred.drop('attr_val'))

        inferred_count = inferred.count()

        precision = float((inferred_count - incorrect_count)) / inferred_count
        recall = 1.0 - (float(remaining_incorrect.count()) / errors.count())
        print ("The precision that we have is :" + str(precision))
        print ("The recall that we have is :" + str(recall))

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
