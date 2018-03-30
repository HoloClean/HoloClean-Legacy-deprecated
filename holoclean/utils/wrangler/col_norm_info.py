import distance


class ColNormInfo:

    def __init__(self, col_name, distance_fcn=distance.levenshtein,
                 threshold=None):
        self.col_name = col_name
        self.distance_fcn = distance_fcn

        if threshold is not None:
            self.threshold = threshold
        elif distance_fcn == distance.levenshtein:
            self.threshold = 3
        else:
            raise ValueError("Requires a threshold if not using levenshtein"
                             "distance")
