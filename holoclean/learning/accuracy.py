from holoclean.global_variables import GlobalVariables
from holoclean.utils.reader import Reader
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

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

    def accuracy_calculation(self):

        if self.session.inferred_values is None:
            self.session.holo_env.logger.error('No Inferred values')
            print ("The precision and recall cannot be calculated")

        else:
            self.read_groundtruth()

            checkable_inferred_query = "SELECT I.tid,I.attr_name," \
                                       "I.attr_val FROM " + \
                                       self.dataset.table_specific_name(
                                           'Inferred_Values') + " AS I , " + \
                                       self.dataset.table_specific_name(
                                           'Groundtruth') + " AS C WHERE " \
                                                            "C.tid=I.tid " \
                                                            "AND " \
                                                            "C.attr_name= " \
                                                            "I.attr_name"

            inferred = self.dataengine.query(checkable_inferred_query, 1)

            checkable_original_query = "SELECT I.tid,I.attr_name," \
                                       "I.attr_val FROM " + \
                                       self.dataset.table_specific_name(
                                           'Observed_Possible_Values_dk') + \
                                       " AS I , " + \
                                       self.dataset.table_specific_name(
                                           'Groundtruth') + " AS C WHERE " \
                                                            "C.tid=I.tid " \
                                                            "AND " \
                                                            "C.attr_name= " \
                                                            "I.attr_name"

            init = self.dataengine.query(checkable_original_query, 1)

            if self.session.simple_predictions:
                inferred_singlevalues = \
                    self.session.simple_predictions.select("tid", "attr_name",
                                                           "attr_val")

            else:
                self.session.holo_env.logger.error('No Simple values')
                inferred_singlevalues = None

            inferred_multivalues = inferred.subtract(inferred_singlevalues)

            # get the simple predictions that we can check

            inferred_singlevalues = inferred.subtract(inferred_multivalues)

            incorrect_singlevalues_count = 0
            original_singlevalues_errors_count = 0
            uncorrected_singlevalues_count = 0

            incorrect_multivalues_count = 0
            original_multivalues_errors_count = 0
            uncorrected_multivalues_count = 0

            inferred_multivalues_count = inferred_multivalues.count()

            if inferred_multivalues_count:
                incorrect_multivalues = inferred_multivalues.subtract(
                    self.ground_truth_flat)
                incorrect_multivalues_count = incorrect_multivalues.count()

                self.dataengine.add_db_table("wrong_multi",
                                             incorrect_multivalues,
                                             self.dataset)
                # inferred_singlevalues has the intitial values
                # (will use it as init for single values)

                original_multivalues_errors = init.subtract(
                    inferred_singlevalues).subtract(self.ground_truth_flat)
                original_multivalues_errors_count = \
                    original_multivalues_errors.count()

                uncorrected_multivalues = original_multivalues_errors.drop(
                    'attr_val').intersect(
                    incorrect_multivalues.drop('attr_val'))

                uncorrected_multivalues_count = uncorrected_multivalues.count()

                if original_multivalues_errors_count:
                    multivalues_recall = \
                        1.0 - (
                            float(uncorrected_multivalues_count
                                  ) / original_multivalues_errors_count)
                else:
                    multivalues_recall = 1.0
                multivalues_precision = float(
                    inferred_multivalues_count - incorrect_multivalues_count
                ) / inferred_multivalues_count

                print ("The multiple-values precision that we have is :" + str(
                    multivalues_precision))
                print ("The multiple-values recall that we have is :" + str(
                    multivalues_recall) + " out of " + str(
                    original_multivalues_errors_count))

            inferred_singlevalues_count = inferred_singlevalues.count()
            if inferred_singlevalues_count:
                incorrect_singlevalues = inferred_singlevalues.subtract(
                    self.ground_truth_flat)

                self.dataengine.add_db_table("wrong_single",
                                             incorrect_singlevalues,
                                             self.dataset)

                # simple predictions has zero recall since it kept all errors

                incorrect_singlevalues_count = incorrect_singlevalues.count()
                original_singlevalues_errors_count = \
                    incorrect_singlevalues_count
                uncorrected_singlevalues_count = incorrect_singlevalues_count

                if original_singlevalues_errors_count:
                    singlevalues_recall = 1.0 - (
                        float(uncorrected_singlevalues_count
                              ) / original_singlevalues_errors_count)
                else:
                    singlevalues_recall = 1.0

                singlevalues_precision = float(
                    inferred_singlevalues_count - incorrect_singlevalues_count
                ) / inferred_singlevalues_count

                print("The single-value precision that we have is :" + str(
                    singlevalues_precision))

                print("The single-value recall that we have is :" + str(
                    singlevalues_recall) + " "
                                           "out of " + str(
                    original_singlevalues_errors_count))


            inferred_count = inferred_multivalues_count + \
                             inferred_singlevalues_count
            if inferred_count:
                incorrect_count = incorrect_multivalues_count + \
                                  incorrect_singlevalues_count
                original_errors_count = \
                    original_multivalues_errors_count + \
                    original_singlevalues_errors_count

                uncorrected_count = uncorrected_multivalues_count + \
                                    uncorrected_singlevalues_count
                if original_errors_count:
                    recall = 1.0 - (float(uncorrected_count) /
                                    original_errors_count)
                else:
                    recall = 1.0
                precision = float((inferred_count - incorrect_count)) / \
                            inferred_count
                print ("The precision that we have is :" + str(precision))

                print ("The recall that we have is :"
                       + str(recall) + " out of " + str(original_errors_count))

    def read_groundtruth(self):
        """Create a dataframe from the ground truth csv file

        Takes as argument the full path name of the csv file
        and the spark_session
        """
        filereader = Reader(self.spark_session)

        groundtruth_schema = StructType([
            StructField("tid", IntegerType(), False),
            StructField("attr_name", StringType(), False),
            StructField("attr_val", StringType(), False)])

        self.ground_truth_flat = filereader.read(self.path_to_grand_truth, 0,
                                                 groundtruth_schema).\
            drop(GlobalVariables.index_name)

        self.dataengine.add_db_table(
            'Groundtruth', self.ground_truth_flat, self.dataset)
