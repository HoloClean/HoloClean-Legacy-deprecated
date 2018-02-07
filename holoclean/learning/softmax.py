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
        return
