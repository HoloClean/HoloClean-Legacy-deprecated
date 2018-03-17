class ErrorDetectorsWrapper:
    """
    This class call different error detection method that we needed
    """

    def __init__(self, detect_obj):
        """
        The general class for error detection

        :param detect_obj: an object which implements
        get_noisy_cells, get_clean_cells
        """
        self.detect_obj = detect_obj
        self.noisy_cells = None
        self.clean_cells = None

    def get_noisy_dknow_dataframe(self):

        """
        Return tuple of noisy cells and clean cells dataframes

        :param data_dataframe: get dataframe of data
        :return: return noisy cells and
        """

        self.noisy_cells = self.detect_obj.get_noisy_cells()
        self.clean_cells = self.detect_obj.get_clean_cells()

        return self.noisy_cells, self.clean_cells
