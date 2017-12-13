from test import Testing
from holoclean.learning.wrapper import Wrapper


class SemanticCompare:

    def __init__(self):
        """TODO.
        Parameters
        --------
        parameter: dataengine,dataset
        """
        self.test = Testing()
        self.dataengine = self.test.holo_obj.dataengine
        self.dataset = self.test.session.dataset

        # Creating two set of edges for comparing
        self.feature_to_factor_graph()
        self.numbskull_to_factor_graph()

    def feature_to_factor_graph(self):

        feature_table_df = self.dataengine._table_to_dataframe("Feature", self.dataset)
        variable_table_df = self.dataengine._table_to_dataframe("Variable", self.dataset)
        variable = []
        for c in variable_table_df.collect():
            if [c[1], c[2]] not in variable:
                variable.append([c[1], c[2]])

        feature_to_fg_edges = set()
        factor_index = 0
        for c in feature_table_df.collect():
            feature_to_fg_edges.add((variable.index([str(c[1]), c[2]]), factor_index))
            factor_index = factor_index + 1

        self.feature_to_fg_edges = feature_to_fg_edges

    def numbskull_to_factor_graph(self):

        wrapper_obj = Wrapper(self.dataengine, self.dataset)
        factor_to_var = list(wrapper_obj.get_list_factor_to_var())
        factors = list(wrapper_obj.get_list_factor())
        numbskull_to_fg_edges = set()
        for factor in factors:
            numbskull_to_fg_edges.add((factor_to_var[factor[4]][0], factors.index(factor)))
        self.numbskull_to_fg_edges = numbskull_to_fg_edges


class CompareUnittest():

    def runTest(self):
        compare = SemanticCompare()
        if len(compare.feature_to_fg_edges - compare.numbskull_to_fg_edges) + \
                len(compare.numbskull_to_fg_edges - compare.feature_to_fg_edges) == 0:
            print 'The graphs is semantically related'
        else:
            print 'The graphs is not semantically related'
