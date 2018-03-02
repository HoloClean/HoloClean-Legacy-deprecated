from holoclean.errordetection.dcerrordetector import DCErrorDetection


class ErrorDetectors:
    """
    This class call different error detection method that we needed
    """

    def __init__(self, DenialConstraints,
                 dataengine,
                 spark_session,
                 dataset,
                 detect_obj = None
                 ):
        """
        In this class we instantiate a DC error detector and pass dataengine to
        fill correspondence tables in data base
        :param DenialConstraints: list of denial constraints
        :param dataengine: list of dataengine
        :param spark_session: spark session
        :param dataset: dataset object for accessing tables name
        :param detection_type: type of errordetection
        """
        if detect_obj is None:
            self.detect_obj = DCErrorDetection(DenialConstraints,
                                               dataengine,
                                               dataset,
                                               spark_session)
        else:
            self.detect_obj = detect_obj

    # Setters:

    # Getters:

    def get_noisy_dknow_dataframe(self, data_dataframe):

        """
        Return tuple of noisy cells and clean cells dataframes
        :param data_dataframe: get dataframe of data
        :return: return noisy cells and
        """

        noisy_cells = self.detect_obj.get_noisy_cells(data_dataframe)
        clean_cells = self.detect_obj.get_clean_cells(data_dataframe,
                                                      noisy_cells)

        return noisy_cells, clean_cells
