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
            self.session.holo_env.logger.error('No inferred values')
            print ("The precision and recall cannot be calculated")

        else:
            self.read_groundtruth()

            checkable_inferred_query = "SELECT I.tid,I.attr_name," \
                                       "I.attr_val, G.attr_val as g_attr_val "\
                                       "FROM " + \
                                       self.dataset.table_specific_name(
                                           'Inferred_Values') + " AS I , " + \
                                       self.dataset.table_specific_name(
                                           'Groundtruth') + " AS G WHERE " \
                                                            "I.tid=G.tid " \
                                                            "AND " \
                                                            "G.attr_name= " \
                                                            "I.attr_name"

            inferred = self.dataengine.query(checkable_inferred_query, 1)

            if inferred is None:
                self.session.holo_env.logger.error('No checkable inferred '
                                                   'values')
                print ("The precision and recall cannot be calculated")
                return


            checkable_original_query = "SELECT I.tid,I.attr_name," \
                                       "I.attr_val, G.attr_val as " \
                                       "g_attr_val FROM " + \
                                       self.dataset.table_specific_name(
                                           'Observed_Possible_Values_dk') + \
                                       " AS I , " + \
                                       self.dataset.table_specific_name(
                                           'Groundtruth') + " AS G WHERE " \
                                                            "I.tid=G.tid " \
                                                            "AND " \
                                                            "G.attr_name= " \
                                                            "I.attr_name"

            init = self.dataengine.query(checkable_original_query, 1)

            correct_inferred = \
                inferred.where(inferred.attr_val ==
                               inferred.g_attr_val).\
                    drop("attr_val","g_attr_val")

            incorrect_inferred = \
                inferred.drop("attr_val","g_attr_val").subtract(
                    correct_inferred).distinct()

            incorrect_init = \
                init.where(init.attr_val != init.g_attr_val).drop(
                    "attr_val","g_attr_val")

            correct_count = correct_inferred.count()
            incorrect_count = incorrect_inferred.count()
            inferred_count = correct_count + incorrect_count
            incorrect_init_count = incorrect_init.count()

            if inferred_count:
                precision = float(correct_count) / float(inferred_count)
                print ("The top-" + str(self.session.holo_env.k_inferred) +
                       " precision  is : " + str(precision))
                uncorrected_inferred = incorrect_init.intersect(
                    incorrect_inferred)
                uncorrected_count = uncorrected_inferred.count()
                if incorrect_init_count:
                    recall = 1.0 - (float(uncorrected_count) /
                                   float(incorrect_init_count))
                else:
                    recall = 1.0
                print ("The top-" + str(self.session.holo_env.k_inferred) +
                       " recall is : " + str(recall) + " out of " + str(
                    incorrect_init_count))

            # Report the MAP accuracy if you are predicting more than 1 value
            if self.session.holo_env.k_inferred > 1:
                checkable_map_query = "SELECT I.tid,I.attr_name," \
                                           "I.attr_val, G.attr_val as " \
                                      "g_attr_val  " \
                                           "FROM " + \
                                           self.dataset.table_specific_name(
                                               'Inferred_map') + " AS I , " \
                                                                 "" + \
                                           self.dataset.table_specific_name(
                                               'Groundtruth') + " AS G " \
                                                                "WHERE " \
                                                                "I.tid= " \
                                                                "G.tid " \
                                                                "AND " \
                                                                "G.attr_name =" \
                                                                "I.attr_name"
                inferred_map = self.dataengine.query(checkable_map_query, 1)
                correct_map = \
                    inferred_map.where(inferred_map.attr_val ==
                                       inferred_map.g_attr_val).drop(
                        "attr_val", "g_attr_val")
                incorrect_map = \
                    inferred_map.drop("attr_val", "g_attr_val").subtract(
                        correct_map).distinct()
                correct_map_count = correct_map.count()
                incorrect_map_count = incorrect_map.count()
                inferred_map_count = correct_map_count + incorrect_map_count

                if inferred_map_count:
                    map_precision = float(correct_map_count) / float(
                        inferred_map_count)
                    print ("The  MAP precision  is : " + str(map_precision))
                    uncorrected_map = incorrect_init.intersect(
                        incorrect_map)
                    uncorrected_map_count = uncorrected_map.count()
                    if incorrect_init_count:
                        recall = 1.0 - (float(uncorrected_map_count) /
                                        float(incorrect_init_count))
                    else:
                        recall = 1.0
                    print ("The MAP recall is : " + str(recall) + " out of " +
                           str(
                        incorrect_init_count))

    def read_groundtruth(self):

        """
        Create a dataframe from the ground truth csv file

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
