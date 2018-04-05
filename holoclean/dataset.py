import random
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


class Dataset:
    """
    This class keep the name of tables in Holoclean project
    """

    def __init__(self):
        """
                The constructor for Dataset class
        """
        self.attribute = {}
        self.schema = ""
        self.dataset_tables_specific_name = []
        self.dataset_id = self._id_generator()
        self.attributes = {
            'id': [],
            'Init': [],
            'C_clean': [],
            'C_dk': [],
            'C_dk_temp': [],
            'C_dk_temp_null': [],
            'T1_attributes': [],
            'T2_attributes': [],
            'Possible_values':
                StructType([
                    StructField("vid", IntegerType(), True),
                    StructField("tid", IntegerType(), False),
                    StructField("attr_name", StringType(), False),
                    StructField("attr_val", StringType(), False),
                    StructField("observed", IntegerType(), False),
                    StructField("domain_id", IntegerType(), True)
                ]),
            'Observed_Possible_values_clean': [],
            'Observed_Possible_values_dk': [],
            'C_clean_flat':
                StructType([
                    StructField("tid", IntegerType(), False),
                    StructField("attribute", StringType(), False),
                    StructField("value", StringType(), True)
                ]),
            'C_dk_flat':
                StructType([
                    StructField("tid", IntegerType(), False),
                    StructField("attribute", StringType(), False),
                    StructField("value", StringType(), True)
                ]),
            'Kij_lookup':
                StructType([
                    StructField("vid", IntegerType(), True),
                    StructField("tid", IntegerType(), False),
                    StructField("attr_name", StringType(), False),
                    StructField("k_ij", IntegerType(), False),
                ]),
            'Init_join': [],
            'Map_schema':
                StructType([
                    StructField("attr_id", IntegerType(), False),
                    StructField("attribute", StringType(), True)
                ]),
            'Init_flat_join_dk': [],
            'Init_flat_join': [],
            'Feature_id_map': StructType([
                    StructField("feature_ind", IntegerType(), True),
                    StructField("attribute", StringType(), False),
                    StructField("value", StringType(), False),
                    StructField("Type", StringType(), False),
                ]),
            'Sources': [],
            'Sources_temp': [],
            'Attribute_temp': [],
            'Dimensions_clean': [],
            'Dimensions_dk': [],
            'Inferred_values': [],
            'Repaired_dataset': [],
            'Correct': [],
            'Correct_flat': [],
            'Feature':
                StructType([
                    StructField("vid", IntegerType(), False),
                    StructField("assigned_val", IntegerType(), False),
                    StructField("feature", IntegerType(), False),
                    StructField("count", IntegerType(), False)
                ])}

    # Internal methods
    @staticmethod
    def _id_generator():
        """
        This function creates a random id from the system time

        :return: random_str : String
             Generating a string of random numbers
        """
        random_str = str(random.random())[2:]
        return random_str

    def print_id(self):
        """
        Writes dataset id inside dataset_id.txt file and returns that id

        :return: dataset id : String
        """
        fx = open('dataset_id.txt', 'w')
        fx.write(str(self.dataset_id))
        fx.close()
        return str(self.dataset_id)

    def return_id(self):
        """
        Returns dataset id as string

        :return: dataset id : String
                The id of data set
        """
        return str(self.dataset_id)

    def table_specific_name(self, table_general_name):
        """
        Returning the name of the table for this dataset

        :param table_general_name: String
                This the general name of table

        :return: String
                Specific name correspond to table general name
        """
        return table_general_name + "_" + self.return_id()

    def get_schema(self, table_name):
        """
        Returns a copy of a list of attributes for the given table_name

        :param table_name: Name of the table

        :return: list of string if table
        """

        return list(self.attributes[table_name])
