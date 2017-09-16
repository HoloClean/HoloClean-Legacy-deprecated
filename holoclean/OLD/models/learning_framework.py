import tensorflow as tf
import numpy as np
import pandas as pd


import sys
import os


par_path=os.path.dirname(os.path.dirname(__file__))
print(par_path)
sys.path.append(par_path)

import dataEngine as de

  
class LaerningData:
    """This class is learning data object that means have one part for id and one part for label and one part for feature values """
    
    def __init__(self,featurized_dataframe):
        self.learn_data=featurized_dataframe
        pass
    
    # This function give list the label  learn_data
    def get_labels(self):
        return self.learn_data[2]
    
    #This function divide data to 2 part for learning and test and give back 2 LearningData Object
    def generate_train_test_data(self):
        pass
    
    #This get features value part of data
    def get_feature_vectors(self):
        pass

class Learning:
    """This class create learning object that operate over LaerningData object that we gave to it"""
    
    num_of_iteration=1000
    step_size=0.05
    
    def __init__(self,holoclean_data):
        self.holoclean_data=holoclean_data
        
    def set_learning_parameter(self,new_num_of_iteration,new_step_size):
        #With this functions user can change the learning parameter
        self.num_of_iteration=new_num_of_iteration
        self.step_size=new_step_size   
                     
    def learn(self):
        #By calling this function the model will learn and return learned model parameters
        #start_data : Show the column index that data start we might have some columns at the beginning of the table

        # separating the data from the labels
        dt_labels = [] # What is the value for each domain
        dt_vals = [] # Values for features
    
        feature_vectors=self.holoclean_data[1]
        label_vectors=self.holoclean_data[2]
        num_of_classes=len(label_vectors[0])
        
        trainshape=feature_vectors.shape
        
        for d in range(0,len(self.holoclean_data[0])):
            dt_vals.append(feature_vectors.iloc[d])
            dt_labels.append(label_vectors[d])
        # print(dt_vals)
        td_labels = [] # What is the value for each domain
        td_vals = [] # Values for features
        # testshape=testData.shape
        # for d in range(0,testshape[0]):
        #     td_vals.append(testData.iloc[d][start_data:(testshape[1]-1)])
        #     td_labels.append(testData.iloc[d][(testshape[1]-1)])
    
    
        #Define number of classes and dimention of input data
        dimension_of_train_data = trainshape[1]
    
        # Make placeholder for X 'feature vector' with specified shape
        x = tf.placeholder(tf.float32, shape=[None, dimension_of_train_data])
    
        #Define Weights and Bias of our model
        W = tf.Variable(tf.zeros([dimension_of_train_data, num_of_classes]))
        # Bias of size num_of_classes
        b = tf.Variable(tf.zeros([num_of_classes]))
    
    
        #Specify learning model as Softmax
        y = tf.nn.softmax(tf.matmul(x, W) + b)
    
    
        #  one-hot array of the dimension of the classed vector indicating what is the correct answer
        y_ = tf.placeholder(tf.float32, shape=[None, num_of_classes])
    
    
        # Specify the loss function
        cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels=y_, logits=y))
    
    
        # Train the model
        train_step = tf.train.GradientDescentOptimizer(self.step_size).minimize(cross_entropy)
    
        # Define session for run learning process
        session = tf.InteractiveSession()
    
        # Run session
        session.run(tf.global_variables_initializer())
    
        for _ in range(self.num_of_iteration):
            train_step.run(feed_dict={x: dt_vals, y_: dt_labels})
    
        correct_prediction = tf.equal(tf.argmax(y, 1), tf.argmax(y_, 1))
        accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
    
        # prec=accuracy.eval(feed_dict={x: td_vals, y_: td_labels})
        Weights=session.run(W)
        bias=session.run(b)
    
        return Weights,bias
            
    def set_learning_param(self,iteration,step):
        #this method change the preset learning parameter that in tensorflow presented 
        self.num_of_iteration=iteration
        self.step_size=step
    
    def cell_label_value_prob(self,query_cell,weights,bias):
        
        s=weights.shape
        probability=[0]*len(bias)
        probability=np.matmul(query_cell, weights) + bias
        
        return probability[1:]
    
    def all_inference_probability(self,probability,query_cell,dictionary):
        ranged=dictionary[query_cell[1]]
        indexes=list(ranged.values())
        tmp=probability[indexes]
        if min(tmp)<0:
            tmp=tmp+abs(min(tmp))
            normalized=tmp/sum(tmp)
        else:
            normalized=tmp/sum(tmp)
#         max_ind=self.max_index(probability,indexes)
#         rev_dict={v: k for k, v in range.items()}
#         pred_val=rev_dict[max_ind]
        rev_dict={v: k for k, v in ranged.items()}
        result=[]
        for i in range(0,len(indexes)):
            result.append((rev_dict[indexes[i]],normalized[i]))
        return  sorted(result, key=lambda x: x[1], reverse=True)
    
    def max_inference(self,probability,query_cell,dictionary):
        range=dictionary[query_cell[1]]
        indexes=list(range.values())
        max_ind=self.max_index(probability,indexes)
        rev_dict={v: k for k, v in range.items()}
        pred_val=rev_dict[max_ind]
        
        return pred_val
    
    def max_inference(self,probability,query_cell,dictionary):
        range=dictionary[query_cell[1]]
        indexes=list(range.values())
        max_ind=self.max_index(probability,indexes)
        rev_dict={v: k for k, v in range.items()}
        pred_val=rev_dict[max_ind]
        
        return pred_v      
    
    def max_index(self,prob,ind):
            
            max_val = 0
            max_ind = 0
             
            for i in range(0,len(ind)):           
                if prob[ind[i]] > max_val:                
                    max_val=prob[ind[i]]               
                    max_ind=ind[i]
            
            return max_ind
    
    
    
    

#This class make some evaluation for learning part

class Evaluation:

    """ This class defines all the evaluation measure that we """
    
    def __init__(self,model_parameters,test_data):
        self.model_parameters=model_parameters
        self.test_data=test_data
    
    def precision(self):
        pass
    
    def recall(self):
        pass






