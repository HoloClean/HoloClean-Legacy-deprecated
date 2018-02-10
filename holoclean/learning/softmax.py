import torch

'''class CustomLogReg(torch.nn.Module):
    def __init__(self, input_dim, output_dim, Weights):
        ## don't need softmax here since cross entropy loss does it for us
        super(CustomLogReg, self).__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim, bias=False)
        self.input_dim = input_dim
        self.W = Weights

    def forward(self, X):
        return TensorProducts.apply(X, self.W, self.input_dim) 

class TensorScores(Function):
    def forward(ctx, X, W, input_dim):
        ## do weights times X. need to mask classes when less than L for this domain
        ##we go down the n indices, computing scores as we go along
        ret = torch.zeros(0,self.input_dim)
        for i in range(X.size()[0]):
            #grab a slice of the tensor
            torch.cat([ret, self.W.mul(X[i]).t().mm(torch.ones(input_dim, 1))], 0)
        return ret
    def backward(ctx, grad_ouput):
        
        grad_W = 0
        return None, grad_W, None
'''
class SoftMax:

    def __init__(self, dataengine, dataset):
        self.dataengine = dataengine
        self.dataset = dataset
        dataframe_offset = self.dataengine.get_table_to_dataframe("Dimensions_clean", self.dataset)
        list = dataframe_offset.collect()
        dimension_dict = {}
        for dimension in list:
            dimension_dict[dimension['dimension']] = dimension['length']


        # X Tensor Dimensions (N * M * L)
        self.M = dimension_dict['M']
        self.N = dimension_dict['N']                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
        self.L = dimension_dict['L']

        # pytorch tensors
        self.X = None
        self._setupX()
        self.mask = None
        self._setupMask()
      #  self.W = None
      #  self._setupW()
        
        return

    # Will create the X-value tensor of size nxmxl
    def _setupX(self):
        coordinates = torch.LongTensor()
        values = torch.LongTensor([])
        feature_table = self.dataengine.get_table_to_dataframe("Feature_clean", self.dataset).collect()
        for factor in feature_table:
            coordinate = torch.LongTensor([[int(factor.vid) - 1], [int(factor.feature) - 1],
                                           [int(factor.assigned_val) - 1]])
            coordinates = torch.cat((coordinates, coordinate), 1)
            value = factor['count']
            values = torch.cat((values, torch.LongTensor([value])), 0)
        self.X = torch.sparse.LongTensor(coordinates, values, torch.Size([self.N, self.M, self.L]))
        #print(self.X.to_dense())
        return

    def _setupMask(self, clean = 1):
        possible_values = "Possible_values_clean" if clean else "Possible_values_dk"


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

    '''def train(model, loss, optimizer, x_val, y_val):
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
'''
