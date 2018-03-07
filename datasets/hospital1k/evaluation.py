import pandas as pd
import numpy as np


class Evaluation:
    def __init__(self, grandtruth_path, holoclean_path, initial_path):
        """
        This class implements all performance evaluation with output of the data
        :param grandtruth_path: list of data for the true data
        :param holoclean_path: the output of the holoclean
        :param initial_path: path to initial data
        """
        self.gt_df = pd.read_csv(grandtruth_path)
        self.holo_df = pd.read_csv(holoclean_path)
        self.init_df = pd.read_csv(initial_path)

    def tuple_type(self):

        self.error_cells = self.init_df[~self.init_df.isin(self.gt_df).all(1)]
        self.correct_cells = self.init_df[self.init_df.isin(self.gt_df).all(1)]
        self.true_positive_alg = pd.merge(self.holo_df, self.gt_df, how='inner', on=['ind', 'attr', 'val'])
        self.false_positive = self.holo_df.shape[0] - self.true_positive_alg.shape[0]
        self.tmp1 = pd.merge(self.true_positive_alg, self.correct_cells, how='inner', on=['ind', 'attr', 'val'])
        self.false_negative = self.correct_cells.shape[0] - self.tmp1.shape[0]
        self.tmp1 = pd.merge(self.true_positive_alg, self.correct_cells, how='inner', on=['ind', 'attr', 'val'])
        self.false_negative = self.correct_cells.shape[0] - self.tmp1.shape[0]

        fixed_cells = self.init_df.shape[0]-self.holo_df.shape[0]

        prec = float(self.true_positive_alg.shape[0] + fixed_cells) / (
                fixed_cells + self.true_positive_alg.shape[0] +
                self.false_positive)

        recall = float(self.true_positive_alg.shape[0] + fixed_cells) / (
                fixed_cells + self.true_positive_alg.shape[0] +
                self.false_negative)

        print "The accuracy is : " + str(100 * prec)
        print "The recall is : " + str(100 * recall)
        print "The F1 is : " + str(100 * 2 * float(prec * recall) / (prec + recall))


eval3 = Evaluation('grandtruth.csv', 'holoclean-tou-0.3.csv', 'input_data.csv')
eval5 = Evaluation('grandtruth.csv', 'holoclean-tou-0.5.csv', 'input_data.csv')
eval3.tuple_type()
eval5.tuple_type()
