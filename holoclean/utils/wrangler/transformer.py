class Transformer:
    """
    This class takes a list of functions
    """
    def __init__(self, functions, columns):
        """
        Initilizing transformer object

        :param functions: list of funtions
        :param columns: list of columns
        """
        self.functions = functions
        self.columns = columns

    def transform(self, df):
        """
        Apply each function in order to every value in the dataframe

        :param df: original dataframe

        :return: transformed dataframe
        """
        new_df = df
        for attr in self.columns:
            for fn in self.functions:
                new_df = new_df.withColumn(attr, fn(new_df[attr]))

        return new_df
