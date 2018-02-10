import torch
import torch.nn as nn
from torch.nn import Parameter
from torch.autograd import Variable
import torch.nn.functional as F
import math


class LogReg(torch.nn.Module):

    # inits weights to random values
    # ties init and dc weights if specified
    def _setup_weights(self):
        
        # setup init
        if (self.tie_init):
            self.init_W = Parameter(torch.randn(1).expand(self.output_dim, 1))
        else:
            self.init_W = Parameter(torch.randn(self.output_dim, 1))
            
        # setup cooccur
        self.cooc_W = Parameter(torch.randn(self.output_dim, self.input_dim_non_dc - 1))
        
        self.W = torch.cat((self.init_W, self.cooc_W), 1)
        
        # setup dc
        if self.input_dim_dc > 0:
            if (self.tie_dc):
                self.dc_W = Parameter(torch.randn(self.input_dim_dc).expand(self.output_dim, self.input_dim_dc))
            else:
                self.dc_W = Parameter(torch.randn(self.output_dim, self.input_dim_dc))
            
            self.W = torch.cat((self.W, self.dc_W), 1)
    
    
    def __init__(self, input_dim_non_dc, input_dim_dc, output_dim, tie_init, tie_dc):
        super(LogReg, self).__init__()
        
        self.input_dim_non_dc = input_dim_non_dc
        self.input_dim_dc = input_dim_dc
        self.output_dim = output_dim
        
        self.tie_init = tie_init
        self.tie_dc = tie_dc

        self._setup_weights()
        
        
    def forward(self, X, index, mask):

        # reties the weights - need to do on every pass
        if self.input_dim_dc > 0:
            self.W = torch.cat((self.init_W.expand(self.output_dim, 1), self.cooc_W,
                               self.dc_W.expand(self.output_dim, self.input_dim_dc)), 1)
        else:
            self.W = torch.cat((self.init_W.expand(self.output_dim, 1), self.cooc_W), 1)
            
            
        # calculates n x l matrix output
        output = X.mul(self.W)
        output = output.sum(2)
        
        # changes values to extremely negative and specified indices
        if index is not None and mask is not None:
            output.index_add_(0, index, mask)
            
        return output
    
class SoftMax:

    def __init__(self, dataengine, dataset):
        self.dataengine = dataengine
        self.dataset = dataset
        query = "SELECT COUNT(*) AS dc FROM " + \
                self.dataset.table_specific_name("Feature_id_map") + \
                " WHERE Type = 'DC'"
        self.DC_count = self.dataengine.query(query, 1).collect()[0].dc
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
        self.Y = None
        self._setupY()
        
        return
    # Will create the Y tensor of size NxL
    def _setupY(self):
        possible_values = self.dataengine.get_table_to_dataframe("Observed_Possible_values_clean", self.dataset).collect()
        self.Y = torch.zeros(self.N, self.L)
        for value in possible_values:
            self.Y[value.vid - 1, value.domain_id - 1] = 1
        print(self.Y)
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
        print(self.X.to_dense())
        return

    def _setupMask(self, clean = 1):
        lookup = "Kij_lookup_clean" if clean else "Kij_lookup_clean"
        K_ij_lookup = self.dataengine.get_table_to_dataframe(
            lookup, self.dataset).select("vid", "k_ij").collect()
        self.mask = torch.zeros(self.N, self.L)
        for domain in K_ij_lookup:
            if domain.k_ij < self.L:
                self.mask[domain.vid-1, domain.k_ij:] = -10e6;
        print(self.mask)
        return


    def build_model(self, input_dim_non_dc, input_dim_dc, output_dim, tie_init=True, tie_DC=True):
        model = LogReg(input_dim_non_dc, input_dim_dc, output_dim, tie_init, tie_DC)
        return model

    def train(self, model, loss, optimizer, x_val, y_val, mask=None):
        x = Variable(x_val, requires_grad=False)
        y = Variable(y_val, requires_grad=False)
    
        if mask is not None:
            mask = Variable(mask, requires_grad=False)
    
        index = torch.LongTensor(range(x_val.size()[0]))
        index = Variable(index, requires_grad=False)

        # Reset gradient
        optimizer.zero_grad()

        # Forward
        fx = model.forward(x, index, mask)

        output = loss.forward(fx, y)

        # Backward
        output.backward()
        
        # Update parameters
        optimizer.step()

        return output.data[0]

    def predict(self, model, x_val):
        x = Variable(x_val, requires_grad=False)
        output = model.forward(x, None, None)     
        return output.data.numpy()

    def logreg(self):

        # here's where the most changes came in from the isolated notebook version
        # hard for me to test anything related to HC implementation until rest is done
        
        ## TODO:
        # setup Y
        # fill dc count once we decide where we're getting that from
        # debug
        
        n_examples, n_features, n_classes = self.X.size()

        # need to fill this with dc_count once we decide where to get that from
        model = self.build_model(self.M - self.DC_count, self.DC_count, n_classes)
        loss = torch.nn.CrossEntropyLoss(size_average=True)
        optimizer = optim.SGD(model.parameters(), lr=0.01, momentum=0.9)

        # experiment with different batch sizes. no hard rule on this
        batch_size = n_examples / 100
        for i in range(100):
            cost = 0.
            num_batches = n_examples // batch_size
            for k in range(num_batches):
                start, end = k * batch_size, (k + 1) * batch_size
                cost += train(model, loss, optimizer, self.X[start:end], self.Y[start:end], self.mask)
        

        return predict(model, self.X)
