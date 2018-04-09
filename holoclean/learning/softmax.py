import torch
from torch.nn import Parameter, ParameterList
from torch.autograd import Variable
from torch import optim
from torch.nn.functional import softmax
from pyspark.sql.types import *
from tqdm import tqdm
import numpy as np


class LogReg(torch.nn.Module):
    """
    Class to generate weights
    """

    def _setup_weights(self):
        """
        Initializes weight tensor with random values
        ties init and dc weights if specified

        :return: Null
        """
        torch.manual_seed(42)
        # setup init
        self.weight_tensors = ParameterList()
        self.tensor_tuple = ()
        self.feature_id = []
        self.W = None
        for featurizer in self.featurizers:
            self.feature_id.append(featurizer.id)
            if featurizer.id == 'SignalInit':
                if self.tie_init:
                    signals_W = Parameter(torch.randn(1).expand(
                        1, self.output_dim))
                else:
                    signals_W = Parameter(torch.randn(1, self.output_dim))
            elif featurizer.id == 'SignalDC':
                if self.tie_dc:
                    signals_W = Parameter(
                        torch.randn(featurizer.count, 1).expand(
                            -1, self.output_dim))
                else:
                    signals_W = Parameter(
                        torch.randn(featurizer.count, self.output_dim))
            else:
                signals_W = \
                    Parameter(
                        torch.randn(
                            featurizer.count, 1).expand(-1, self.output_dim))
            self.weight_tensors.append(signals_W)

        return

    def __init__(self, featurizers, input_dim_non_dc, input_dim_dc, output_dim,
                 tie_init, tie_dc):
        """
        Constructor for our logistic regression

        :param input_dim_non_dc: number of init + cooccur features
        :param input_dim_dc: number of dc features
        :param output_dim: number of classes
        :param tie_init: boolean, determines weight tying for init features
        :param tie_dc: boolean, determines weight tying for dc features
        """
        super(LogReg, self).__init__()

        self.featurizers = featurizers

        self.input_dim_non_dc = input_dim_non_dc
        self.input_dim_dc = input_dim_dc
        self.output_dim = output_dim

        self.tie_init = tie_init
        self.tie_dc = tie_dc

        self._setup_weights()

    def forward(self, X, index, mask):
        """
        Runs the forward pass of our logreg.

        :param X: values of the features
        :param index: indices to mask at
        :param mask: tensor to remove possibility of choosing unused class
        :return: output - X * W after masking
        """

        # Reties the weights - need to do on every pass

        self.concat_weights()

        # Calculates n x l matrix output
        output = X.mul(self.W)
        output = output.sum(1)
        # Changes values to extremely negative at specified indices
        if index is not None and mask is not None:
            output.index_add_(0, index, mask)
        return output

    def concat_weights(self):
        """
        Reties the weight
        """
        for feature_index in range(0, len(self.weight_tensors)):
            if self.feature_id[feature_index] == 'SignalInit':
                tensor = self.weight_tensors[feature_index].expand(
                    1, self.output_dim)
            elif self.feature_id[feature_index] == 'SignalDC':
                tensor = self.weight_tensors[feature_index].expand(
                    -1, self.output_dim)
            else:
                tensor = self.weight_tensors[feature_index].expand(
                    -1, self.output_dim)
            if feature_index == 0:
                self.W = tensor + 0
            else:
                self.W = torch.cat((self.W, tensor), 0)


class SoftMax:

    def __init__(self, session, X_training):
        """
        Constructor for our softmax model

        :param X_training: x tensor used for training the model
        :param session: session object

        """
        self.session = session
        self.dataengine = session.holo_env.dataengine
        self.dataset = session.dataset
        self.holo_obj = session.holo_env
        self.spark_session = self.holo_obj.spark_session
        query = "SELECT COUNT(*) AS dc FROM " + \
                self.dataset.table_specific_name("Feature_id_map") + \
                " WHERE Type = 'DC'"
        self.DC_count = self.dataengine.query(query, 1).collect()[0].dc
        dataframe_offset = self .dataengine.get_table_to_dataframe(
            "Dimensions_clean", self.dataset)
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
        self.mask = None
        self.testmask = None
        self.setupMask()
        self.Y = None
        self.grdt = None
        self._setupY()
        self.model = None

    # Will create the Y tensor of size NxL
    def _setupY(self):
        """
        Initializes a y tensor to compare to our model's output

        :return: Null
        """
        possible_values = self.dataengine .get_table_to_dataframe(
            "Observed_Possible_values_clean", self.dataset) .collect()
        self.Y = torch.zeros(self.N, 1).type(torch.LongTensor)
        for value in possible_values:
            self.Y[value.vid - 1, 0] = value.domain_id - 1
        self.grdt = self.Y.numpy().flatten()
        return

    # Will create the X-value tensor of size nxmxl
    def _setupX(self, sparse=0):
        """
        Initializes an X tensor of features for prediction

        :param sparse: 0 if dense tensor, 1 if sparse

        :return: Null
        """
        feature_table = self .dataengine.get_table_to_dataframe(
            "Feature_clean", self.dataset).collect()
        if sparse:
            coordinates = torch.LongTensor()
            values = torch.FloatTensor([])
            for factor in feature_table:
                coordinate = torch.LongTensor([[int(factor.vid) - 1],
                                               [int(factor.feature) - 1],
                                               [int(factor.assigned_val) - 1]])
                coordinates = torch.cat((coordinates, coordinate), 1)
                value = factor['count']
                values = torch.cat((values, torch.FloatTensor([value])), 0)
            self.X = torch.sparse\
                .FloatTensor(coordinates, values,
                             torch.Size([self.N, self.M, self.L]))
        else:
            self.X = torch.zeros(self.N, self.M, self.L)
            for factor in feature_table:
                self.X[factor.vid - 1, factor.feature - 1,
                       factor.assigned_val - 1] = factor['count']
        return

    def setuptrainingX(self, sparse=0):
        """
        Initializes an X tensor of features for training

        :param sparse: 0 if dense tensor, 1 if sparse

        :return: x tensor of features
        """
        dataframe_offset = self.dataengine.get_table_to_dataframe(
            "Dimensions_dk", self.dataset)
        list = dataframe_offset.collect()
        dimension_dict = {}
        for dimension in list:
            dimension_dict[dimension['dimension']] = dimension['length']

        # X Tensor Dimensions (N * M * L)
        self.testM = dimension_dict['M']
        self.testN = dimension_dict['N']
        self.testL = dimension_dict['L']

        feature_table = self.dataengine.get_table_to_dataframe(
            "Feature_dk", self.dataset).collect()
        if sparse:
            coordinates = torch.LongTensor()
            values = torch.FloatTensor([])
            for factor in feature_table:
                coordinate = torch.LongTensor([[int(factor.vid) - 1],
                                               [int(factor.feature) - 1],
                                               [int(factor.assigned_val) - 1]])
                coordinates = torch.cat((coordinates, coordinate), 1)
                value = factor['count']
                values = torch.cat((values, torch.FloatTensor([value])), 0)
            X = torch.sparse.FloatTensor(coordinates, values, torch.Size(
                [self.testN, self.testM, self.testL]))
        else:
            X = torch.zeros(self.testN, self.testM, self.testL)
            for factor in feature_table:
                X[factor.vid -
                  1, factor.feature -
                  1, factor.assigned_val -
                  1] = factor['count']
        return X

    def setupMask(self, clean=1, N=1, L=1):
        """
        Initializes a masking tensor for ignoring impossible classes

        :param clean: 1 if clean cells, 0 if don't-know
        :param N: number of examples
        :param L: number of classes

        :return: masking tensor
        """
        lookup = "Kij_lookup_clean" if clean else "Kij_lookup_dk"
        N = self.N if clean else N
        L = self.L if clean else L
        K_ij_lookup = self.dataengine.get_table_to_dataframe(
            lookup, self.dataset).select("vid", "k_ij").collect()
        mask = torch.zeros(N, L)
        for domain in K_ij_lookup:
            if domain.k_ij < L:
                mask[domain.vid - 1, domain.k_ij:] = -10e6
        if clean:
            self.mask = mask
        else:
            self.testmask = mask
        return mask

    def build_model(self,  featurizers, input_dim_non_dc, input_dim_dc,
                    output_dim, tie_init=True, tie_DC=True):
        """
        Initializes the logreg part of our model

        :param input_dim_non_dc: number of init + cooccur features
        :param featurizers: list of featurizers
        :param input_dim_dc: number of dc features
        :param output_dim: number of classes
        :param tie_init: boolean to decide weight tying for init features
        :param tie_DC: boolean to decide weight tying for dc features

        :return: newly created LogReg model
        """
        model = LogReg(
            featurizers,
            input_dim_non_dc,
            input_dim_dc,
            output_dim,
            tie_init,
            tie_DC,)
        return model

    def train(self, model, loss, optimizer, x_val, y_val, mask=None):
        """
        Trains our model on the clean cells

        :param model: logistic regression model
        :param loss: loss function used for evaluating performance
        :param optimizer: optimizer for our neural net
        :param x_val: x tensor - features
        :param y_val: y tensor - output for comparison
        :param mask: masking tensor

        :return: cost of traininng
        """
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

        output = loss.forward(fx, y.squeeze(1))

        # Backward
        output.backward()

        # Update parameters
        optimizer.step()

        return output.data[0]

    def predict(self, model, x_val, mask=None):
        """
        Runs our model on the test set

        :param model: trained logreg model
        :param x_val: test x tensor
        :param mask: masking tensor to restrict domain

        :return: predicted classes with probabilities
        """
        x = Variable(x_val, requires_grad=False)

        index = torch.LongTensor(range(x_val.size()[0]))
        index = Variable(index, requires_grad=False)

        if mask is not None:
            mask = Variable(mask, requires_grad=False)

        output = model.forward(x, index, mask)
        output = softmax(output, 1)
        return output

    def logreg(self, featurizers):
        """
        Trains our model on clean cells and predicts vals for clean cells

        :return: predictions
        """
        n_examples, n_features, n_classes = self.X.size()
        self.model = self.build_model(
            featurizers, self.M - self.DC_count, self.DC_count, n_classes)
        loss = torch.nn.CrossEntropyLoss(size_average=True)
        optimizer = optim.SGD(
            self.model.parameters(),
            lr=self.holo_obj.learning_rate,
            momentum=self.holo_obj.momentum,
            weight_decay=self.holo_obj.weight_decay)

        # Experiment with different batch sizes. no hard rule on this
        batch_size = self.holo_obj.batch_size
        for i in tqdm(range(self.holo_obj.learning_iterations)):
            cost = 0.
            num_batches = n_examples // batch_size
            for k in range(num_batches):
                start, end = k * batch_size, (k + 1) * batch_size
                cost += self.train(self.model,
                                   loss,
                                   optimizer,
                                   self.X[start:end],
                                   self.Y[start:end],
                                   self.mask[start:end])
            predY = self.predict(self.model, self.X, self.mask)
            map = predY.data.numpy().argmax(axis=1)

            if self.holo_obj.verbose:
                print("Epoch %d, cost = %f, acc = %.2f%%" %
                      (i + 1, cost / num_batches,
                       100. * np.mean(map == self.grdt)))
        return self.predict(self.model, self.X, self.mask)

    def save_prediction(self, Y):
        """
        Stores our predicted values in the database

        :param Y: tensor with probability for each class

        :return: Null
        """
        max_result = torch.topk(Y,self.session.holo_env.k_inferred,1)
        max_indexes = max_result[1].data.tolist()
        max_prob = max_result[0].data.tolist()

        vid_to_value = []
        df_possible_values = self.dataengine.get_table_to_dataframe(
            'Possible_values_dk', self.dataset).select(
            "vid", "attr_name", "attr_val", "tid", "domain_id")

        # Save predictions upt to the specified k unless Prob = 0.0
        for i in range(len(max_indexes)):
                for j in range(self.session.holo_env.k_inferred):
                    if max_prob[i][j]:
                        vid_to_value.append([i + 1, max_indexes[i][j] + 1,
                                             max_prob[i][j]])
        df_vid_to_value = self.spark_session.createDataFrame(
            vid_to_value, StructType([
                StructField("vid1", IntegerType(), False),
                StructField("domain_id1", IntegerType(), False),
                StructField("probability", DoubleType(), False)
            ])
        )
        df1 = df_vid_to_value
        df2 = df_possible_values
        df_inference = df1.join(
            df2, [
                df1.vid1 == df2.vid,
                df1.domain_id1 == df2.domain_id], 'inner')\
            .drop("vid1","domain_id1")

        self.session.inferred_values = df_inference
        self.session.holo_env.logger.info\
            ("The Inferred_values Dataframe has been created")
        self.session.holo_env.logger.info("  ")
        return

    def log_weights(self):
        """
        Writes weights in the logger

        :return: Null
        """
        self.model.concat_weights()
        weights = self.model.W
        self.session.holo_env.logger.info("Tensor weights")
        count = 0
        for weight in \
                torch.index_select(
                    weights, 1, Variable(torch.LongTensor([0]))
                ):

            count += 1
            msg = "Feature " + str(count) + ": " + str(weight[0].data[0])
            self.session.holo_env.logger.info(msg)

        return
