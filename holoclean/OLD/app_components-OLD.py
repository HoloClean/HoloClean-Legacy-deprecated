from pyspark.sql import SparkSession
import os
import sys
par_path=os.path.dirname(os.path.dirname(__file__))
sys.path.append(par_path+"/holoclean/utilities")
sys.path.append(par_path+"/holoclean/models")

import learning_framework as lf
import data_parser as dp

class Session:
    
    running_project=[]
    ##############Session Control###################
    #This function start a new session
#     def __init__(self,session_config,session_name):
#         self.session_config=session_config
#         self.session_name=session_name

    #Start session with configure
    def start(self):
        
        spark = SparkSession.builder.getOrCreate()
        
        return spark
        
    #Finish the session
    def finish(self):
        pass
    
    ################################################

    ##########Project Control Methods################

    #This method add a project to the session
    def add_project(self,project_id):
        self.running_project.append(project_id)

    #This method kill a project by its id
    def kill_project_by_id(self,project_id):

        pass

    #This function define the number of projects running on the session
    def number_of_runnig_project(self):

        pass
    ##################################################

class Project:
    
    def __init__(self,name,database_address,session_obj):
        self.name=name
        self.database_address=database_address
        self.session_obj=session_obj

    #This method do the error detection part
    def detect_errors(self):
        pass
    #This method do the error repairing by using Holoclean
    def repair_errors(self):
        parse=dp.DatasetParser()
        ext=parse.data_format(self.database_address)
        if ext=='csv':
            
            df=self.session_obj.read.csv(self.database_address,header=True)
            
        print(df)  
    
class Dataset:
    """ This class have 2 type of constructor and define the path of dataset path """    
    
    ##### Initilize method #####
    def __init__(self,dataset_path):
        self.dataset_path=dataset_path #data path 

        
    def __init__(self,dataset_path,denial_constraints,noisy_cells):
        self.noisy_cells=noisy_cells#List of index like {0 'attribute'}
        self.dataset_path=dataset_path #data path 
        self.denial_constraints=denial_constraints  # List of denial Constraint objects

    #This function get the directory of noisy cells stored with error detectionEngine and change it to list and return it
    def load_noisy(self,noisy_cells_list_path):
        self.noisy_cells_list_path=noisy_cells_list_path   #List of index like {0 'attribute'}

    #this function get the directory of constraints and return list of constraint
    def load_Constraints(self,constraints_path):
        self.constraints_path=constraints_path  # path of denial Constraint objects
    
    #########################################################
    
    ##############Data manipulation methods##################
    
    #This function make dataframe from data set that we put its path dataset_path
    def load_to_dataframe(self):
        pass


    #This function update the noisy cells after it get feedback it consumes UserFeedback objects
    def update_noisy_cells(self,user_feedback):
        pass

    #This function create 2 dimension dictionary that for each attribute and value specify the index of it in our one-hot label
    #Dictionary : {'c': {'k': 5, 'l': 6}, 'b': {1: 2, 3: 3, 4: 4}, 'a': {1: 0, 3: 1}}  'a'  ,'b' ,'c' are the attribute and enterior key are the value of that attribute
    #Because the label doesn't belong to just one domain we need to have dictionary to keep each attribute values index  
    def make_label_dictionary(self):
        pass

    #This method make label for a give cell based on the dictionary that we created 
    #This function take a cell and by using dictionary return a one-hot list which show the value of that cell  
    def make_label_for_cell(self,cell_index):
        pass

    #This part of code make dataframe with three columns first part : index ,second part : feature vector,Third part :data label
    def make_dataframe_for_featurize_cell(self):
        pass

    #return list of clean tuples index in the given data_set (index,attribute)it act over datafram of given dataset
    def clean_tuples(self,new_attribute_domain,dataframe):
        pass

    #This function based on the domain pruning policy return list of index that can be in the train data 
    def train_candidate(self,domain):
        pass
    ######################################################
    
class Feedback:
    
    ###############Preparing to show result###############
    confidence_level=0.8 # We preset the value for confidence level but user can change it
    def __init__(self,confidence_level):
        self.confidence_level=pred

    #This function get user confidence level
    def get_user_confidence_level(self):
        pass
    
    #######################################################




    ###############Interact with user################
    #This function make questionnaire for user it is something like
    # [
    #     (23,'City'),[]
    #     (12111,'PO.Box'),[]
    #     (34,'City'),[]
    #     (545,'Salary'),[]
    #     (85943,'id'),[]
    #  ]
    #User Can put value in []
    def make_questionnaire(self):
        pass


    #This function show the constraints that have error  in the data
    def show_error_in_constraints(self):
        pass

    #This function get questionnaire answer and pass it to FeedbackParser
    def get_user_answer_to_questionnare(self):
        pass
    ######################################################

class ErrorMessages:
    
    ###########Get control from user ##################### 
    #this message return result that created with correct constraints
    def continued_with_correct_ones(self):
        pass
    ######################################################
    
    ###############Make diagnosing information ###########
    #this function return proper message with constraints that have error in our policy
    def constraint_error(self):
        pass

    #Error with noisy cells
    def noisy_cells_error(self):
        pass

    #This function show the proper message to user about dataset corruption
    def input_dataset_errors(self):
        pass
    ######################################################
    
ses=Session().start()
proj=Project('proj1','/home/alireza/git/holoclean/holoclean/test.csv',ses).repair_errors()
