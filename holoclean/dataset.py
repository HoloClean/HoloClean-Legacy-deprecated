import random
from datetime import datetime


class Dataset:
    attributes = [
        'id',
        'Init',
        'C_clean',
        'C_dk',
        'Possible_values_clean',
        'Possible_values_dk',
        'Observed_Possible_values_clean',
        'Observed_Possible_values_dk',
        'C_clean_flat',
        'C_dk_flat',
        'Kij_lookup_clean',
        'Kij_lookup_dk',
        'Init_join',
        'Map_schema',
        'Init_flat_join_dk',
        'Init_flat_join',
        'Feature_id_map',
        'Sources',
        'Sources_temp',
        'Attribute_temp',
        'Dimensions_clean',
        'Dimensions_dk',
        'Inferred_values',
        'Repaired_dataset',
        'Correct',
        'Correct_flat']

    """

        Each element stand for some data that the holoclean needs or create:

            id : is the unique id for the dataset and it will be used in registering and retrieving data
            Init : the initial data that get to the database from a file that user give
            C_clean : it is table with index of clean cells
            C_dk : is table of indices that we don't know they are noisy or clean
            Possible_values_clean: is the table of all possible values for the clean cells
            Possible_values_dk: is the table of all possible values for the do not know cell
            Observed_Possible_values_clean : is the table with the observed values for the clean cells
            Observed_Possible_values_dk : is the table with the observed values for the do now know cells
            C_clean_flat: is the table for the clean cells that are flatted on three columns index, attribute, and value
            C_dk_flat: is the table for the dk cells that are flatted on three columns index, attribute, and value
            Kij_lookup_clean: is the table with  the lenght of the domain for each clean cell
            Kij_lookup_dk:is the table with  the lenght of the domain for each do not know cell
            Init_flat_join:is the  self join of C_clean_flat table
            Init_flat_join_dk: is the self join of C_dk_flat table
            Map_schema:  is the table with the schema of the Init table
            Feature_id_map: is the table that maps each feature to a number
            Sources: is the table that maps each source to a number
            Sources_temp: is a temporary table for saving the sources
            Attribute_temp: is a temporary table for saving the attributes
            Dimensions_clean: is  a table with the dimensions for the X tensor for training
            Dimensions_dk: is a table with the dimensions for the X tensor for learning
            Inferred_values: is the tables with the inferred values
            Repaired_dataset: is the dataset table after we apply repairs to initial data
            Correct: is the table with the correct values for our dataset
            Correct_flat: is th table with the correct data that are flatted on three columns index, attribute,
            and value

        """

    def __init__(self):
        """TODO.

                    Parameters
                    ----------
                    parameter : type
                    This is a parameter

                    Returns
                    -------
                    describe : type
                        Creates the table_names for each attribute for the dataset
                    """

        self.attribute = {}
        self.dataset_tables_specific_name = []
        for a in Dataset.attributes:
            self.attribute[a] = 0
        self.dataset_id = self._id_generator()
        self.dataset_tables_specific_name.append(self.dataset_id)
        for i in range(1, len(self.attributes)):
            self.dataset_tables_specific_name.append(
                self.dataset_id + '_' + self.attributes[i])

    # Internal methods
    def _id_generator(self):
        """This function create
                a random id from the system time
        """

        r = random.seed(datetime.now())
        return str(random.random())[2:]

    def print_id(self):
        fx = open('dataset_id.txt', 'w')
        fx.write(str(self.dataset_id))
        fx.close()
        return str(self.dataset_id)

    def return_id(self):
        return str(self.dataset_id)

    # Getters

    def getattribute(self, attr):
        return self.dataset_tables_specific_name[attr]

    def table_specific_name(self, table_general_name):
        """TODO return the name of the table for this dataset"""
        return self.dataset_tables_specific_name[self.attributes.index(
            table_general_name)]

    # Setters
