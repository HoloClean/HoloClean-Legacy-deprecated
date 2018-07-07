class ErrorDetectorsWrapper:
    """
    This class calls different error detection methods
    """

    def __init__(self, detect_obj):
        """
        Constructor for error detector wrapper

        :param detect_obj: detector object
        """
        self.detect_obj = detect_obj

    def get_noisy_dknow_dataframe(self):

        """
        Returns tuple of noisy (don't know) cells and clean cells dataframes

        :return:  noisy (i.e. don't know) cells
        :return:  clean cells
        """

        noisy_cells = self.detect_obj.get_noisy_cells()
        clean_cells = self.detect_obj.get_clean_cells()

        return noisy_cells, clean_cells
