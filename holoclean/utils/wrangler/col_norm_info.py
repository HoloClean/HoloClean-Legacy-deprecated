import distance


class ColNormInfo:
    """
    Class to keep the normalization information
    """

    def __init__(self, col_name, distance_fcn=distance.levenshtein,
                 threshold=None):
        """
        Constructing column information

        :param col_name: name of column
        :param distance_fcn: distance function
        :param threshold: threshold for clustering
        """
        self.col_name = col_name
        self.distance_fcn = distance_fcn

        if threshold is not None:
            self.threshold = threshold
        elif distance_fcn == distance.levenshtein:
            self.threshold = 3
        else:
            raise ValueError("Requires a threshold if not using levenshtein"
                             "distance")
