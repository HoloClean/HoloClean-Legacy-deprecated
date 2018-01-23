import random
from datetime import datetime


class Dataset:
    attributes = [
        'id',
        'Init',
        'Init_flat',
        'C_clean',
        'C_clean_flat',
        'Training_flat',
        'C_dk',
        'C_dk_flat',
        'Feature',
        'Feature_temp',
        'Feature_gb',
        'Feature_gb_accur',
        'weight_temp',
        'Possible_values',
        'Weights',
        'Biases',
        'k_Probabilities',
        'Probabilities',
        'Variable',
        'Variable_tmp',
        'Factor',
        'Factor_to_var',
        'Final',
        'Training',
        'Correct_flat',
        'Correct',
        'Fact',
        'Sources']

    """

        Each element stand for some data that the holoclean needs or create:

            id : is the unique id for the dataset and it will be used in registering and retrieving data
            Init : the initial data that get to the database from a file that user give
            Feature : table of feature vector each row of it is feature vector for a cell with id of indices it has
            size of cells in the T 
            Feature_temp : feature table that used to make map for weight id 
            Feature_gb : keep the smallest index in the feature table each random variable starts 
            Feature_gb_accur : keep the accuracy for each rvs 
            Probabilities : is the table of probabilities for don't know cells
            k_Probabilities: it holds the k values with the biggest probability
            Variable : using Variable_temp and fill offset column
            Variable_temp : is the table for the wrapper of variables for numbskull
            Factor: is the table for the wrapper of factor for numbskull
            Weights : table of weights that we learn in the learning section for numbskull
            Factor_to_var: table of factor_to_var for numbskull
            Final: table with final results in order to check the accuracy
            Correct: table with the correct values for our dataset
            Correct_flat: table with the correct values with the form key, attribute, value
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

    # Getters
    def getattribute(self, attr):
        return self.dataset_tables_specific_name[attr]

    def table_specific_name(self, table_general_name):
        """TODO return the name of the table for this dataset"""
        return self.dataset_tables_specific_name[self.attributes.index(
            table_general_name)]

    # Setters
