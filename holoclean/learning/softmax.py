import torch.sparse

class CustomLogReg(torch.nn.Module):
    def __init__(self, input_dim, output_dim):
        ## don't need softmax here since cross entropy loss does it for us
        super(CustomLogReg, self).__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim, bias=False)

    def forward(self, X):
        ## do weights times X. need to mask classes when less than L for this domain
        pass


class SoftMax:

    def __init__(self, dataengine, dataset):
        self.dataengine = dataengine
        self.dataset = dataset
        dataframe_offset = self.dataengine.get_table_to_dataframe("offset", self.dataset)
        list = dataframe_offset.collect()
        offset_dict = {}
        for offset in list:
            offset_dict[offset['offset_type']] = offset['offset']

        self.init_count = offset_dict['Init']
        self.cooccur_count = offset_dict['Cooccur']
        self.DC_count = offset_dict['Dc']

        # X Tensor Dimensions (N * M * L)
        self.M = self.init_count+self.cooccur_count + self.DC_count
        self.N = offset_dict['N']
        self.L = offset_dict['max_domain']

        # pytorch tensors
        self.X = None
        self._setupX()

        self.W = None
        self._setupW()
        
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
        self.X = torch.sparse.LongTensor(coordinates, values, torch.Size([self.N, self.M, self.L]))
        #print(self.X.to_dense())
        return

    # creates the W tensor of size m x l
    def _setupW(self):
        # Theo said all weights for one feature should start the same
        # Not tied for the init/cooccur but should still begin at same point
        
        # set up weight matrix for non DC cols. These weights are not tied
        non_DC_W = torch.randn(self.M - self.DC_count, self.L).type(torch.LongTensor)

        # set up weight matrix for DCs with weights tied along the row
        DC_col = torch.randn(self.DC_count).type(torch.LongTensor)
        DC_W = DC_col.repeat(self.L, 1).t()
        
        self.W = torch.cat((non_DC_W, DC_W), 0)

        return

    def train(model, loss, optimizer, x_val, y_val):
        x = Variable(x_val, requires_grad=False)
        y = Variable(y_val, requires_grad=False)

        # Reset gradient
        optimizer.zero_grad()

        fx = model.forward(x)
        output = loss.forward(fx, y)

        output.backward()

        optimizer.step()

        return output.data[0]

    def predict(model, x_val):
        x = Variable(x_val, requires_grad=False)
        output = model.forward(x)
        return output.data.numpy().argmax(axis=1)

    def main():

        clean_table = self.dataengine.get_table_to_dataframe("C_clean", self.dataset).collect()
        
        n_samples = self.N
        n_features = self.M
        n_classes = self.L
        
        model = CustomLogReg(n_samples, n_classes)
        loss = torch.nn.CrossEntropyLoss(size_average=True)
        optimizer = optim.SGD(model.parameters(), lr=0.01, momentum=0.9)

        batch_size = 100

        for i in range(100):
            cost = 0.
            num_batches = n_samples // batch_size
            for k in range(num_batches):
                start, end = k * batch_size, (k + 1) * batch_size
                cost += train(model, loss, optimizer, X[start:end, :, :], 'I dont know what to put here')
            
