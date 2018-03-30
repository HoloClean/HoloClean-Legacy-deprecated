class Transformer:

    # takes a list of functions
    def __init__(self, functions, columns):
        self.functions = functions
        self.columns = columns

    # apply each function in order to every value in the dataframe
    def transform(self, df):
        new_df = df
        for attr in self.columns:
            for fn in self.functions:
                new_df = new_df.withColumn(attr, fn(new_df[attr]))

        return new_df
