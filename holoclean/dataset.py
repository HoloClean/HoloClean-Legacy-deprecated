import random


class Dataset:
    attributes = [
        'id',
        'Init',
        'C_clean_temp',
        'C_dk_temp',
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

            id : unique id for the dataset and it will be used in
             registering and retrieving data
            Init : initial data that get to the database from a file
             that user gives
            C_clean : table with index of clean cells
            C_dk : is table of indices that we don't know they
             are noisy or clean
            Possible_values_clean:  table of all possible values
             for the clean cells
            Possible_values_dk: is the table of all possible
             values for the do not know cell
            Observed_Possible_values_clean : table with the observed
             values for the clean cells
            Observed_Possible_values_dk : table with the observed
             values for the do now know cells
            C_clean_flat: table for the clean cells that are
             flatted on three columns index, attribute, and value
            C_dk_flat: table for the dk cells that are flatted
             on three columns index, attribute, and value
            Kij_lookup_clean: table with  the lenght of the
             domain for each clean cell
            Kij_lookup_dk: table with  the lenght of the domain
             for each do not know cell
            Init_flat_join: self join of C_clean_flat table
            Init_flat_join_dk: self join of C_dk_flat table
            Map_schema: table with the schema of the Init table
            Feature_id_map: table that maps each feature to a number
            Sources: table that maps each source to a number
            Sources_temp: a temporary table for saving the sources
            Attribute_temp: a temporary table for saving the attributes
            Dimensions_clean: a table with the dimensions for the
             X tensor for training
            Dimensions_dk: a table with the dimensions for the
             X tensor for learning
            Inferred_values: tables with the inferred values
            Repaired_dataset: dataset table after we apply
             repairs to initial data
            Correct: table with the correct values for our dataset
            Correct_flat: table with the correct data that
             are flatted on three columns index, attribute, and value
        """

    def __init__(self):
        """
        The constructor for Dataset class

        Parameters
        ----------
        No parameter

        Returns
        -------
        No Return
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
    @staticmethod
    def _id_generator():
        """
        This function create a random id from the system time

        Parameters
        ----------
        No parameter

        Returns
        -------
        :return: random_str : String
             Generating a string of random numbers
        """
        random_str = str(random.random())[2:]
        return random_str

    def print_id(self):
        """
        Writes dataset id inside dataset_id.txt file and returns that id

        Parameters
        ----------
        No parameter

        Returns
        -------
        :return: dataset id : String
        """
        fx = open('dataset_id.txt', 'w')
        fx.write(str(self.dataset_id))
        fx.close()
        return str(self.dataset_id)

    def return_id(self):
        """
        Returns detaset id as string

        Parameters
        ----------
        No parameter

        Returns
        -------
        :return: dataset id : String
                The id of data set
        """
        return str(self.dataset_id)

    # Getters

    def get_specific_name_by_index(self, table_index):
        """
        Returning the table specific name using index

        Parameters
        ----------
        :param table_index: String
                The index of table we want its specific name

        Returns
        -------
        :return: table specific name : String
                The specific name of table
        """
        return self.dataset_tables_specific_name[table_index]

    def table_specific_name(self, table_general_name):
        """
        Returning the name of the table for this dataset

        Parameters
        ----------
        :param table_general_name: String
                This the general name of table

        Returns
        -------
        :return: String
                Specific name correspond to table general name
        """
        return self.dataset_tables_specific_name[self.attributes.index(
            table_general_name)]

    # Setters
