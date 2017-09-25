import pyspark as ps
import pandas as pd
import dataengine as de
import ingest
import dataset


class HolocleanSession:
    
    def __init__(self,spark_session):
        self.spark_session=spark_session


 
ds=dataset.Dataset()
d=de.Dataengine("metadb-config.txt",'datadb-config.txt',ds)
a=ingest.Ingest("10.csv")
a.reader(d)
