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

    def get_noisy_dknow_dataframe(self):

        """
        Return tuple of noisy cells and clean cells dataframes

        :return: return noisy cells and
        """

        noisy_cells = self.detect_obj.get_noisy_cells()
        clean_cells = self.detect_obj.get_clean_cells()

        return noisy_cells, clean_cells
