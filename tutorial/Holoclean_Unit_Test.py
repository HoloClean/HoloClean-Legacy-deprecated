
# coding: utf-8

# # A Step-by-Step Guide to Holoclean example

# Noisy and erroneous data is a major bottleneck in analytics. Data cleaning and repairing account for about 60% of the work of data scientists. To address this bottleneck, we recently introduced HoloClean, a semi-automated data repairing framework that relies on statistical learning and inference to repair errors in structured data. In HoloClean, we build upon the paradigm of weak supervision and demonstrate how to leverage diverse signals, including user-defined heuristic rules (such as generalized data integrity constraints) and external dictionaries, to repair erroneous data. 

# In this post, we walk through the process of implementing Holoclean, by creating a simple end-to-end example.

# # Setup

# Firstly, we import all the module from Holoclean that we will use.

# In[1]:


from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.errordetector import ErrorDetectors
from holoclean.featurization.featurizer import SignalInit, SignalCooccur, SignalDC
from holoclean.featurization.featurizer import Featurizer
from holoclean.learning.softmax import SoftMax
from holoclean.learning.accuracy import Accuracy
import time


# ##   Initialization
# In this part, we create the Holoclean and Session object that we will use for this example.

# In[2]:


holo_obj = HoloClean(mysql_driver = "../holoclean/lib/mysql-connector-java-5.1.44-bin.jar" )
session = Session("Session", holo_obj)
        


# ## Read Input and DC from file
# Test data and the Denial Constraints will be read using the Session's ingestor.
# After ingesting the test data will be loaded into MySQL tables along with entries in the a metadata table.

# In[3]:


dataset ="../datasets/unit_test/unit_test_dataset.csv"

denial_constraints = "../datasets/unit_test/unit_test_constraints.txt"

# Ingesting Dataset and Denial Constraints
start_time = time.time()
t0 = time.time()
session.ingest_dataset(dataset)
t1 = time.time()
total = t1 - t0


print 'time for ingesting file: ' + str(total) + '\n'
session.denial_constraints(denial_constraints)
print 'Init table'
sql = holo_obj.dataengine.get_table_to_dataframe("Init", session.dataset)

sql.show()


# ## Error Detection
# In this part, we create the error detection. The output of this part is the C_dk table that contains all the noisy cells and the C_Clean table that contains the clean cells

# In[4]:


t0 = time.time()
err_detector = ErrorDetectors(session.Denial_constraints, holo_obj.dataengine,
                             holo_obj.spark_session, session.dataset)
session.add_error_detector(err_detector)
session.ds_detect_errors()

t1 = time.time()
total = t1 - t0
holo_obj.logger.info('error dectection time: '+str(total)+'\n')
print 'error dectection time: '+str(total)+'\n'


# ## Domain Pruning
# In this part, we prune the domain. The output of this part is the possible_values tables that contains all the possible values for each cell

# In[5]:


t0 = time.time()
pruning_threshold = 0.5
session.ds_domain_pruning(pruning_threshold)

t1 = time.time()
total = t1 - t0
holo_obj.logger.info('domain pruning time: '+str(total)+'\n')
print 'domain pruning time: '+str(total)+'\n'

print 'Possible_values_clean'
sql = holo_obj.dataengine.get_table_to_dataframe("Possible_values_clean", session.dataset)
sql.show()

print 'Possible values dk'
sql = holo_obj.dataengine.get_table_to_dataframe("Possible_values_dk", session.dataset)
sql.show()


# # Featurization

# In this part, we implement the featurization module of holoclean. We choose the signals that we want to use and the output of this part is the featurization table that contains the factors that we will use

# ## Feature Signals

# In[6]:


t0 = time.time()
initial_value_signal = SignalInit(session.Denial_constraints, holo_obj.dataengine,
                              session.dataset)
session.add_featurizer(initial_value_signal )
statistics_signal = SignalCooccur(session.Denial_constraints, holo_obj.dataengine,
                              session.dataset )
session.add_featurizer(statistics_signal)
dc_signal = SignalDC(session.Denial_constraints, holo_obj.dataengine, session.dataset,
                 holo_obj.spark_session)
session.add_featurizer(dc_signal)
t1 = time.time()
total = t1 - t0
print "Feature Signal Time:", total


# We use the signals that we choose in the previous step. The output of this part is the featurization table that contains the factors that we will use in the next step.

# In[7]:


t0 = time.time()
session.ds_featurize()

t1 = time.time()

total = t1 - t0

holo_obj.logger.info('featurization time: '+str(total)+'\n')
print 'featurization time: '+str(total)+'\n'


# #  Learning
# We create the X-tensor from the feature_clean table and run softmax on it

# In[8]:


t0 = time.time()
soft = SoftMax(holo_obj.dataengine, session.dataset, holo_obj.spark_session,
                       session.X_training)

soft.logreg()
t1 = time.time()
total = t1 - t0

print 'time for training model: '+str(total)+'\n'


# In this part, we use the new weight, to learn the probabilities for each value for the cells
# 

# In[9]:


t0 = time.time()
session.ds_featurize(0)
t1 = time.time()
total = t1 - t0
print 'time for test featurization: ' + str(total) + '\n'

Y = soft.predict(soft.model, session.X_testing, soft.setupMask(0, session.N, session.L))
t1 = time.time()
total = t1 - t0
print 'time for inference: ', total
soft.save_prediction(Y)

print 'Inferred values for dk cells'
sql = holo_obj.dataengine.get_table_to_dataframe("Inferred_values", session.dataset)
sql.show()

endtime = time.time()
print 'total time: ', endtime - start_time

