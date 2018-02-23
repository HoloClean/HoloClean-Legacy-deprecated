import torch
import torch.nn as nn
from torch.nn import Parameter
from torch.autograd import Variable
import torch.nn.functional as F
import math
from torch import optim
from torch.nn.functional import softmax
from pyspark.sql.types import *
from pyspark.sql.functions import first

class LogReg(torch.nn.Module):

    # inits weights to random values
    # ties init and dc weights if specified
    def _setup_weights(self):
        torch.manual_seed(42)
        # setup init
        if (self.tie_init):
            self.init_W = Parameter(torch.randn(1).expand(1, self.output_dim))
        else:
            self.init_W = Parameter(torch.randn(1, self.output_dim))

        # setup cooccur
        self.cooc_W = Parameter(torch.randn(self.input_dim_non_dc - 1, 1).expand(-1, self.output_dim))

        self.W = torch.cat((self.init_W, self.cooc_W), 0)

        # setup dc
        if self.input_dim_dc > 0:
            if (self.tie_dc):
                self.dc_W = Parameter(torch.randn(self.output_dim).expand(self.input_dim_dc, self.output_dim))
            else:
                self.dc_W = Parameter(torch.randn(self.input_dim_dc, self.output_dim))

            self.W = torch.cat((self.W, self.dc_W), 0)
    
    
    def __init__(self, input_dim_non_dc, input_dim_dc, output_dim, tie_init, tie_dc, rv_dim):
        super(LogReg, self).__init__()
        
        self.input_dim_non_dc = input_dim_non_dc
        self.input_dim_dc = input_dim_dc
        self.output_dim = output_dim
        
        self.tie_init = tie_init
        self.tie_dc = tie_dc
        self.rv_dim = rv_dim

        self._setup_weights()
        
        
    def forward(self, X, index, mask):

        # reties the weights - need to do on every pass
        if self.input_dim_dc > 0:
            self.W = torch.cat((self.init_W.expand(1, self.output_dim), self.cooc_W,
                                self.dc_W.expand(self.input_dim_dc, self.output_dim)), 0)
        else:
            self.W = torch.cat((self.init_W.expand(1, self.output_dim), self.cooc_W), 0)
            
            
        # calculates n x l matrix output
        output = X.mul(self.W)
        output = output.sum(1)
        
        # changes values to extremely negative and specified indices
        if index is not None and mask is not None:
            output.index_add_(0, index, mask)
            
        return output
    
class SoftMax:

    def __init__(self, dataengine, dataset, spark_session, X_training):
        self.dataengine = dataengine
        self.dataset = dataset
        self.spark_session = spark_session
        query = "SELECT COUNT(*) AS dc FROM " + \
                self.dataset.table_specific_name("Feature_id_map") + \
                " WHERE Type = 'DC'"
        print self.dataengine.query(query, 1).show()
        print self.dataengine.query(query, 1).collect()
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

        self.testM = None
        self.testN = None
        self.testL = None

        # pytorch tensors
        self.X = X_training
        #self._setupX()
        self.mask = None
        self.testmask = None
        self.setupMask()
        self.Y = None
        self._setupY()

        self.model = None
        return
    # Will create the Y tensor of size NxL
    def _setupY(self):
        possible_values = self.dataengine.get_table_to_dataframe("Observed_Possible_values_clean", self.dataset).collect()
        self.Y = torch.zeros(self.N, 1).type(torch.LongTensor)
        for value in possible_values:
            self.Y[value.vid - 1, 0] = value.domain_id - 1
        return

    # Will create the X-value tensor of size nxmxl
    def _setupX(self, sparse=0):
        feature_table = self.dataengine.get_table_to_dataframe("Feature_clean", self.dataset).collect()
        if sparse:
            coordinates = torch.LongTensor()
            values = torch.FloatTensor([])
            for factor in feature_table:
                coordinate = torch.LongTensor([[int(factor.vid) - 1], [int(factor.feature) - 1],
                                               [int(factor.assigned_val) - 1]])
                coordinates = torch.cat((coordinates, coordinate), 1)
                value = factor['count']
                values = torch.cat((values, torch.FloatTensor([value])), 0)
            self.X = torch.sparse.FloatTensor(coordinates, values, torch.Size([self.N, self.M, self.L]))
        else:
            self.X = torch.zeros(self.N, self.M, self.L)
            for factor in feature_table:
                self.X[factor.vid - 1, factor.feature - 1, factor.assigned_val - 1] = factor['count']
        return

    def setuptrainingX(self, sparse=0):
        dataframe_offset = self.dataengine.get_table_to_dataframe("Dimensions_dk", self.dataset)
        list = dataframe_offset.collect()
        dimension_dict = {}
        for dimension in list:
            dimension_dict[dimension['dimension']] = dimension['length']

        # X Tensor Dimensions (N * M * L)
        self.testM = dimension_dict['M']
        self.testN = dimension_dict['N']
        self.testL = dimension_dict['L']

        feature_table = self.dataengine.get_table_to_dataframe("Feature_dk", self.dataset).collect()
        if sparse:
            coordinates = torch.LongTensor()
            values = torch.FloatTensor([])
            for factor in feature_table:
                coordinate = torch.LongTensor([[int(factor.vid) - 1], [int(factor.feature) - 1],
                                               [int(factor.assigned_val) - 1]])
                coordinates = torch.cat((coordinates, coordinate), 1)
                value = factor['count']
                values = torch.cat((values, torch.FloatTensor([value])), 0)
            X = torch.sparse.FloatTensor(coordinates, values, torch.Size([self.testN, self.testM, self.testL]))
        else:
            X = torch.zeros(self.testN, self.testM, self.testL)
            for factor in feature_table:
                X[factor.vid - 1, factor.feature - 1, factor.assigned_val - 1] = factor['count']
        return X

    def setupMask(self, clean=1, N=1, L=1):
        lookup = "Kij_lookup_clean" if clean else "Kij_lookup_dk"
        N = self.N if clean else N
        L = self.L if clean else L
        K_ij_lookup = self.dataengine.get_table_to_dataframe(
            lookup, self.dataset).select("vid", "k_ij").collect()
        mask = torch.zeros(N,L)
        for domain in K_ij_lookup:
            if domain.k_ij < L:
                mask[domain.vid-1, domain.k_ij:] = -10e6;
        if clean:
            self.mask = mask
        else:
            self.testmask = mask
        return mask


    def build_model(self, input_dim_non_dc, input_dim_dc, output_dim, tie_init=True, tie_DC=True):
        model = LogReg(input_dim_non_dc, input_dim_dc, output_dim, tie_init, tie_DC, self.N)
        return model

    def train(self, model, loss, optimizer, x_val, y_val, mask=None):
        x = Variable(x_val, requires_grad=False)
        # x = Variable(x_val, requires_grad=False)
        y = Variable(y_val, requires_grad=False)
    
        if mask is not None:
            mask = Variable(mask, requires_grad=False)
    
        index = torch.LongTensor(range(x_val.size()[0]))
        index = Variable(index, requires_grad=False)

        # Reset gradient
        optimizer.zero_grad()

        # Forward
        fx = model.forward(x, index, mask)

        output = loss.forward(fx, y.squeeze(1))

        # Backward
        output.backward()
        
        # Update parameters
        optimizer.step()

        return output.data[0]

    def predict(self, model, x_val, mask=None):

        # x = Variable(x_val, requires_grad=False)
        x = Variable(x_val, requires_grad=False)

        index = torch.LongTensor(range(x_val.size()[0]))
        index = Variable(index, requires_grad=False)

        if mask is not None:
            mask = Variable(mask, requires_grad=False)
        
        output = model.forward(x, index, mask)
        output = softmax(output, 1)
        return output

    def logreg(self):

        # here's where the most changes came in from the isolated notebook version
        # hard for me to test anything related to HC implementation until rest is done
        
        ## TODO:
        # debug
        
        n_examples, n_features, n_classes = self.X.size()

        # need to fill this with dc_count once we decide where to get that from
        self.model = self.build_model(self.M - self.DC_count, self.DC_count, n_classes)
        loss = torch.nn.CrossEntropyLoss(size_average=True)
        optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9)

        # experiment with different batch sizes. no hard rule on this
        batch_size = n_examples
        for i in range(200):
            cost = 0.
            num_batches = n_examples // batch_size
            #for k in range(num_batches):
            #    start, end = k * batch_size, (k + 1) * batch_size
            #    cost += self.train(model, loss, optimizer, self.X[start:end], self.Y[start:end], self.mask)
            cost += self.train(self.model, loss, optimizer, self.X, self.Y, self.mask)
        return self.predict(self.model, self.X, self.mask)

    def save_prediction(self, Y):
        max_result = torch.max(Y, 1)
        max_indexes = max_result[1].data.tolist()
        max_prob = max_result[0].data.tolist()
        vid_to_value = []
        df_possible_values = self.dataengine.get_table_to_dataframe('Possible_values_dk',self.dataset).select(
            "vid", "attr_name", "attr_val", "tid", "domain_id")
        for i in range(len(max_indexes)):
            vid_to_value.append([i+1, max_indexes[i]+1, max_prob[i]])
        df_vid_to_value = self.spark_session.createDataFrame(
            vid_to_value, StructType([
                StructField("vid2", IntegerType(), False),
                StructField("domain_id2", IntegerType(), False),
                StructField("probability", DoubleType(), False)
            ])
        )
        df1 = df_vid_to_value
        df2 = df_possible_values
        df_inference = df1.join(df2, [df1.vid2 == df2.vid, df1.domain_id2 == df2.domain_id], 'inner').drop(
            "vid2", "domain_id2")
        self.dataengine.add_db_table('Inferred_values',
                                     df_inference, self.dataset)
        return


    def repair_init(self):

        # pivot repairs to wide
        inferred = self.dataengine.get_table_to_dataframe('Inferred_values', self.dataset)
        repairs = inferred.groupBy('tid').pivot('attr_name').agg(first('attr_val')).collect()

        repairs = self.spark_session.createDataFrame(repairs)
        repairs.createOrReplaceTempView('Repairs')

        self.dataengine.add_db_table('Repairs', repairs, self.dataset)

        # apply repairs to initial data
        repaired = self.dataengine.get_table_to_dataframe('Init', self.dataset)
        repaired.createOrReplaceTempView('Repaired')
        self.dataengine.add_db_table('Repaired', repaired, self.dataset)

        dirty_attrs = [str(row.attr_name) for row in inferred.select('attr_name').distinct().collect()]

        for attr in dirty_attrs:
            repair_query = 'UPDATE ' + self.dataset.table_specific_name('Repaired') + ' init ' \
                           'LEFT JOIN ' + self.dataset.table_specific_name('Repairs') + ' repairs ' \
                           'ON init.index = repairs.tid ' \
                           'SET init.' + attr + ' = repairs.' + attr + ' ' \
                           'WHERE repairs.' + attr + ' IS NOT NULL;'
            self.dataengine.query(repair_query)
