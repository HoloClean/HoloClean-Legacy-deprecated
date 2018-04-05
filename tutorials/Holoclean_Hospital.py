
# coding: utf-8

# ## Tutorial 2: A Complete HoloClean Pipeline

# In this tutorial, we will walk step-by-step through the process of repairing a dataset in `HoloClean`. 

# The dataset in question contains information about hospitals and is commonly-used for research purposes. Errors are present in ~5% of the cells and there is significant duplicate information - the ideal environment for `HoloClean`.

# ### Step 1: Data Loading

# We begin by instantiating the `HoloClean` and `Session` objects needed to run the repairs. For a more detailed overview of these objects and the rest of our infrastructure, please see our [Data Loading & Denial Constraints Tutorial](Tutorial_1.ipynb).
# To altar exposed variables such as pruning threshold. 
# Send as a parameter when initializing the HoloClean object.
# Examples of exposed variables:
#     Learning Rate: learning_rate
#     Pruning Threshold: pruning_threshold
#     Number of Learning Iterations: learning_iterations
#     Batch Size: batch_size
# For a list of all possible arguements check the holoclean.py file

# In[1]:


from holoclean.holoclean import HoloClean, Session

holo       = HoloClean(
            holoclean_path="..",         # path to holoclean package
            verbose=False,
            # to limit possible values for training data
            pruning_threshold1=0.1,
            # to limit possible values for training data to less than k values
            pruning_clean_breakoff=6,
            # to limit possible values for dirty data (applied after
            # Threshold 1)
            pruning_threshold2=0,
            # to limit possible values for dirty data to less than k values
            pruning_dk_breakoff=6,
            # learning parameters
            learning_iterations=30,
            learning_rate=0.001,
            batch_size=5
        )
session = Session(holo)


# Next, we load in the data and denial constraints needed for this dataset. Both pieces of information are stored in the Postgres database.

# In[2]:


data_path = "data/hospital.csv"

## loads data into our database and returns pyspark dataframe of initial data
data = session.load_data(data_path)

dc_path = "data/hospital_constraints.txt"

# loads denial constraints into our database and returns a simple list of dcs as strings 
dcs = session.load_denial_constraints(dc_path)


# It's easy to see the dataset has a decent amount of errors. Note the random 'x' characters that have been substituted in.

# In[3]:


# all pyspark dataframe commands available
data.select('City').show(15)


# In[4]:


# a simple list of strings
dcs


# ### Step 2: Error Detection

# HoloClean is a supervised error repairing system. In contrast to traditional supervision, we do not ask users to label individual data cells but rely on more high-level supervision signals. These signals are denial constraints or other, custom-made error detectors that split the data into two categories, "clean" and "don't-know". Using that split, our later steps of the process will be able to learn the features of a "clean" cell and perform inference on the values of the "don't-know" cells.

# Please see our <a href=http://pages.cs.wisc.edu/~thodrek/blog/holoclean.html>blog post</a> for more information

# In this tutorial, we will use HoloClean's built in error detector that uses denial constraints to perform this split. Any cell that participates in a violation of a DC is marked "don't-know", the rest are treated as clean. If you wish to develop a custom error detector, please see our [Error Detectors Tutorial](Tutorial_3.ipynb) for a walkthrough.

# In[5]:


from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection

# instantiate Holoclean's built in error detector
detector = SqlDCErrorDetection(session)

# both clean and dirty sets are returned as pyspark dataframes
error_detector_list =[]
error_detector_list.append(detector)
clean, dirty = session.detect_errors(error_detector_list)


# In[6]:


clean.head(5)


# In[7]:


dirty.head(5)


# ### Step 3: Repairing

# With the "clean" and "don't-know" split defined, we are ready to perform repairs.

# Denial Constraints are the driving force behind this process. Denial constraints are used as features in a softmax regression model. The clean cells are used as training examples to learn the parameters (weights) of this model. Once those weights are defined, we use this model to perform inference on the "don't-know" cells and insert the most likely value for each cell.

# This tutorial will simply use the default parameters for our softmax model. Customization of parameters like learning rate, batch size, and number of epochs is described in Tutorial 4 (in development) and is recommended for performance-critical applications.

# In[8]:


repaired = session.repair()


# As we can see, our repaired dataset has effectively removed large numbers of the 'x' characters

# In[9]:


repaired = repaired.withColumn("__ind", repaired["__ind"].cast("int"))
repaired.sort('__ind').select('City').show(15)


# ### Performance Evaluation

# Since this is a research dataset, a clean version is available for us to compare our results to. 

# In[10]:


session.compare_to_truth("data/hospital_clean.csv")

