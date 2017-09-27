import sys
sys.path.append('../')
from holoclean import holocleansession , dataset , dataengine
from holoclean.errordetection import dcerrordetector
from holoclean.utils import dcparser


ds=dataset.Dataset()   
d=dataengine.Dataengine("metadb-config.txt",'datadb-config.txt',ds)
d.ingest('10.csv')
d.register("C_clean","index,attr")
#dcCode=['t1&t2&EQ(t1.city,t2.city)&EQ(t1.temp,t2.temp)&IQ(t1.tempType,t2.tempType)']
#x=dcparser.DCParser(dcCode)
#and_of_preds=x.make_and_condition('all')
# dce=dcerrordetector.DCErrorDetection(dcCode,d)

print(x.get_attribute(and_of_preds[0],d.get_schema('T').split(',')))

# hs=holocleansession.HolocleanSession(d,'local')
# 
# x=d.get_table("T")
# y=hs._covert2_spark_dataframe('T')
# print(x)
# print(y.show())
# print(y.toPandas())
