import numpy as np
from scipy.spatial.distance import squareform
from scipy.cluster.hierarchy import linkage, fcluster
from collections import Counter


class Normalizer:
    """

    """

    def __init__(self, col_infos, max_distinct=1000):
        """
        Initilizing normalizer class

        :param col_infos: columns attribute
        :param max_distinct: maximum distinct values
        """
        self.col_infos = col_infos
        self.dist_dict = {}
        self.max_distinct = max_distinct

    def normalize(self, df):
        """
        Creates normalized dataframe with respect to each column

        :param df: input dataframe

        :return: normalized dataframe
        """

        new_df = df
        for ci in self.col_infos:
            new_df = self._normalize_col(new_df, ci)
        return new_df

    def _normalize_col(self, df, ci):
        """
        Normalizing column in given dataframe

        :param df: input dataframe
        :param ci: column name

        :return: normalized dataframe with respect to given column
        """

        col_name = ci.col_name
        col = df.select(col_name).collect()

        col = [row[col_name].encode('utf-8', 'replace')
               if row[col_name] is not None else ''for row in col]

        distinct = list(set(col))

        if len(distinct) > self.max_distinct or len(distinct) <= 1:
            return df

        similarity = self._compute_distances(distinct, ci.distance_fcn)

        z = linkage(similarity)

        labels = fcluster(z, ci.threshold, 'distance')

        # sets up map from value to most common value in that cluster
        clusters = self._get_exemplars(col, labels, distinct)

        new_col = [clusters[val][0] for val in col]

        df = df.na.replace(col, new_col, col_name)

        return df

    def _compute_distance(self, w1, w2, dist_fcn):
        """
        Computing distance between two strings

        :param w1: string one
        :param w2: string two
        :param dist_fcn: distance function

        :return: distance
        """
        key = frozenset((w1, w2))
        if key in self.dist_dict:
            return self.dist_dict[key]
        else:
            distance = dist_fcn(w1, w2)
            self.dist_dict[key] = distance
            return distance

    def _compute_distances(self, col, dist_fcn):

        """
        Creates distance for all pairs elements in column

        :param col: target column
        :param dist_fcn: distance function

        :return: distance matrix
        """
        distances = np.array([[self._compute_distance(w1, w2, dist_fcn)
                               for w1 in col] for w2 in col])
        return squareform(distances, force='tovector', checks=False)

    def _get_exemplars(self, col, labels, distinct):
        """
        Generates most common values from clustering

        :param col: column
        :param labels: label of elements in column
        :param distinct: distinct values in column

        :return: center of the cluster (most common)
        """
        clusters = dict()
        for label in np.unique(labels):
            cluster_indices = [i for i, val in enumerate(labels)
                               if val == label]

            cluster_distinct = [val for i, val in enumerate(distinct)
                                if i in cluster_indices]

            cluster = [val for val in col if val in cluster_distinct]

            counter = Counter(cluster)
            exemplar = counter.most_common(1)[0]

            for val in cluster:
                clusters[val] = exemplar
        return clusters
