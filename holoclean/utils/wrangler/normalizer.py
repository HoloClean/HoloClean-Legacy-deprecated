import numpy as np
from scipy.spatial.distance import squareform
from scipy.cluster.hierarchy import linkage, fcluster
from collections import Counter


class Normalizer:

    def __init__(self, col_infos, max_distinct=1000):
        self.col_infos = col_infos
        self.dist_dict = {}
        self.max_distinct = max_distinct

    def normalize(self, df):

        new_df = df
        for ci in self.col_infos:
            new_df = self._normalize_col(new_df, ci)
        return new_df

    def _normalize_col(self, df, ci):

        col_name = ci.col_name
        col = df.select(col_name).collect()

        col = [str(row[col_name]) if row[col_name] is not None else ''
               for row in col]

        distinct = list(set(col))

        if len(distinct) > self.max_distinct:
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
        key = frozenset((w1, w2))
        if key in self.dist_dict:
            return self.dist_dict[key]
        else:
            distance = dist_fcn(w1, w2)
            self.dist_dict[key] = distance
            return distance

    def _compute_distances(self, col, dist_fcn):
        distances = np.array([[self._compute_distance(w1, w2, dist_fcn)
                               for w1 in col] for w2 in col])
        return squareform(distances, force='tovector', checks=False)

    def _get_exemplars(self, col, labels, distinct):
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
