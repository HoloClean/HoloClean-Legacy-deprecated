import torch.sparse

class SoftMax:

    def __init__(self, dataengine, dataset):
        self.dataengine = dataengine
        self.dataset = dataset
        dataframe_offset = self.dataengine.get_table_to_dataframe("offset", self.dataset)
        list = dataframe_offset.select('offset').collect()
        self.init_count=list[0].offset
        self.cooccur_count = list[1].offset
        self.DC_count =list[2].offset

        # X Tensor Dimensions (N * M * L)
        self.M = self.init_count+self.cooccur_count + self.DC_count


        # pytorch tensors
        self.X = None
        self._setupX()
        return

    # Will create the X-value tensor of size nxmxl
    def _setupX(self):
        coordinates = torch.LongTensor()
        values = torch.LongTensor([])
        feature_table = self.dataengine.get_table_to_dataframe("Feature", self.dataset).collect()
        for factor in feature_table:
            print(factor)
            feature_index = None
            if factor.TYPE == 'init':
                feature_index = 0
            elif factor.TYPE == 'cooccur':
                feature_index = self.init_count + factor.feature - 1
            elif factor.TYPE == 'FD':
                feature_index = self.init_count + self.cooccur_count + factor.feature - 1

            coordinate = torch.LongTensor([[factor.var_index - 1], [feature_index], [factor.assigned_val - 1]])
            coordinates = torch.cat((coordinates, coordinate), 1)
            value = factor['count']
            values = torch.cat((values, torch.LongTensor([value])), 0)
        self.X = torch.sparse.LongTensor(coordinates, values)
        print(self.X.to_dense())
        return
