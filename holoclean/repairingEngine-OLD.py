class Run:
    #this takes path of data from dataEngine module in style of dataframe
    def get_data(self):
        pass
    #this function start to run repairing data
    def start_run(self):
        pass
    
class Result:

    """This function create performance measures for learned model this class take test data and parameter model and make measurement"""

    model_param=[]
    def __init__(self,model,test_data):
        self.model_param=model
        self.test_data=test_data


    #this method calculate the model accuracy
    def accuracy(self):
        pass 
    #this makes precision for this model     
    def precision(self):
        pass
   #this makes recall for our model
    def recall(self):
        pass
    #this make coverage for our model    
    def coverage(self):
        pass
