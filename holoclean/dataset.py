import random
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType


class Dataset:
    """
    This class keeps the name of tables in Holoclean project
    """

    def __init__(self):
        """
        Each element in attributes stands for some data that the holoclean
         needs to create:
            id : unique id for the dataset and it will be used in
             registering and retrieving data
            Init : initial data that get to the database from a file
             that user gives
            C_clean : table with index of clean cells
            C_dk : table of indices that we don't know if they
             are noisy or clean
            C_dk_temp : table of indices that we don't know if they
             are noisy or clean based on the dcs
            C_dk_temp_null : table of indices that we don't know if they
             are noisy or clean based on the null cells
            T1_attributes:  attributes of the first tuple in dc grounding
            T2_attributes:  attributes of the second tuple in dc grounding
            Possible_values: table of all possible values for the
             do not know cells
            Observed_Possible_values_clean : table with the observed
             values for the clean cells
            Observed_Possible_values_dk : table with the observed
             values for the do not know cells
            C_clean_flat: table for the clean cells that are
             flatted on three columns (index, attribute, and value)
            C_dk_flat: table for the dk cells that are flatted
             on three columns (index, attribute, and value)
            Kij_lookup: table with the cardinality of the
             domain for each cell
            Init_join: self join of init table
            Map_schema: table with the schema of the Init table
            Init_flat_join: self join of C_clean_flat table
            Init_flat_join_dk: self join of C_dk_flat table
            Feature_id_map: table that maps each feature to a number
            Sources: table that maps each source to a number
            Sources_temp: temporary table for saving the sources
            Attribute_temp: temporary table for saving the attributes
            Dimensions_clean: table with the dimensions for the
             X tensor for training
            Dimensions_dk: table with the dimensions for the
             X tensor for learning
            Inferred_values: table with the inferred values
            Repaired_dataset: dataset table after we apply
             repairs to initial data
            Correct: table with the correct values for our dataset
            Correct_flat: table with the correct data that
             are flatted on three columns (index, attribute, and value)
            Feature: table with feature value for each random variable
            and assigned value
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
            'Inferred_values': StructType([
                    StructField("probability", DoubleType(), False),
                    StructField("vid", IntegerType(), False),
                    StructField("attr_name", StringType(), False),
                    StructField("attr_val", StringType(), False),
                    StructField("tid", IntegerType(), False),
                    StructField("domain_id", IntegerType(), False)
                ]),
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
        Returns the name of the table for this dataset

        :param table_general_name: String
                This the general name of table

        :return: String
                Specific name corresponding to table general name
        """
        return table_general_name + "_" + self.return_id()

    def get_schema(self, table_name):
        """
        Returns a copy of a list of attributes for the given table_name

        :param table_name: Name of the table

        :return: list of string if table
        """

        return list(self.attributes[table_name])
