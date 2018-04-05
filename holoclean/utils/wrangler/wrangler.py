class Wrangler:

    def __init__(self):
        """
        Initializing wrangler object
        """
        self.transformers = list()
        self.normalizer = None

    def add_transformer(self, transformer):
        """
        Adds one transformer to the wrangler
        can use multiple since might user might want different sets
        of functions for different sets of columns

        :param transformer: Transformer object

        :return: Null
        """
        self.transformers.append(transformer)

    def add_normalizer(self, normalizer):
        """
        Adds normalizer to the wranger
        only one of these is allowed in our current setup

        :param normalizer: Normalizer object

        :return: Null
        """
        self.normalizer = normalizer

    def wrangle(self, df):
        """
        Transforms and normalizes data using the user defined objects

        :param df: dataframe to wrangle

        :return: wrangled dataframe
        """
        for transformer in self.transformers:
            df = transformer.transform(df)

        if self.normalizer is not None:
            df = self.normalizer.normalize(df)

        return df
