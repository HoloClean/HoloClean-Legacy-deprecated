import pyspark as ps
import pandas as pd
import dataengine as de
import sys
sys.path.append('../')
import dataset
from holoclean.utils import ingest

class HolocleanSession:
    
    def __init__(self,spark_session):
        self.spark_session=spark_session


 
ds=dataset.Dataset()
print(ds.attributes)
d=de.Dataengine("metadb-config.txt",'datadb-config.txt',ds)
a=ingest.Ingest("10.csv")
a.reader(d)
sql_query="Select * from "+ds.table_name[1]
df=d.retrieve(sql_query)

print(d.get_schema("T"))