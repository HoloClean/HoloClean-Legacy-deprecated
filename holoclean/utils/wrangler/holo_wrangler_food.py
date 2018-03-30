
# coding: utf-8

# A quick walkthrough of wrangling a df in HoloClean

# In[23]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = spark.read.csv("../../../datasets/food/food_input_large.csv", header=True, encoding='utf-8')
out_path = "../../../datasets/food/food_input_large_norm.csv"

from wrangler import Wrangler

wrangler = Wrangler()


# In[26]:


from transformer import Transformer
from transform_functions import lowercase, trim

functions = [lowercase, trim]
columns = ["akaname","inspectionid","city","state",
           "results","longitude","latitude","inspectiondate","risk","location",
           "license","facilitytype","address","inspectiontype","dbaname","zip"]

transformer = Transformer(functions, columns)


# In[27]:


wrangler.add_transformer(transformer)


# Our wrangler by default uses levenshtein's distance but it can take any distance function for comparing strings.
# 
# The only trick is you must specify the threshold at which to stop clustering. For example, levenshtein's distance uses a default threshold of 3, so 'chicago' and 'checago' will be clustered but 'chicago' and 'cafcebo' will not. This threshold needs to be chosen depending on the distance function used and the known properties of the column's data.

# In[28]:


from col_norm_info import ColNormInfo
import distance

cols = list()
cols.append(ColNormInfo("City"))
cols.append(ColNormInfo("State", distance.jaccard, 0.7))


# Other than the column information, our normalizer takes the max number of distinct values that we will permit it to compare. Any more than that and the process becomes too time and space intensive so we simply do not normalize any column that fails that condition

# In[29]:


from normalizer import Normalizer

normalizer = Normalizer(cols, max_distinct=1000)


# In[30]:


wrangler.add_normalizer(normalizer)


# In[31]:


wrangled_df = wrangler.wrangle(data)





wrangled_df.toPandas().to_csv(out_path, index=False, header=True,encoding='utf-8')

