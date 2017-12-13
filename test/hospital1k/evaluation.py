import pandas as pd


class Evaluation:
    """
    This class implements all performance evaluation with output of the data
    """
    def __init__(self, grandtruth_path, holoclean_path):
        self.gt_df = pd.read_csv(grandtruth_path)
        self.holo_df = pd.read_csv(holoclean_path)

    def accuracy(self):
        self.correct_ans = pd.merge(self.holo_df, self.gt_df, how='inner', on=['ind', 'attr', 'val'])

        print "The accuracy is : " + str(100 * float(self.correct_ans.shape[0])/self.holo_df.shape[0])


eval3 = Evaluation('grandtruth.csv', 'holoclean-tou-0.3.csv')
eval5 = Evaluation('grandtruth.csv', 'holoclean-tou-0.5.csv')
eval3.accuracy()
eval5.accuracy()
