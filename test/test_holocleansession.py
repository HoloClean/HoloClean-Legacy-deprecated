import sys
sys.path.append('../')
from holoclean import holocleansession , dataset , dataengine
from holoclean.errordetection import dcerrordetector
from holoclean.utils import dcparser


ds=dataset.Dataset()   
d=dataengine.Dataengine("metadb-config.txt",'datadb-config.txt',ds)
d.ingest('10.csv')
dd=d.register("C_clean","ind,attr")


dcCode=['t1&t2&EQ(t1.city,t2.city)&EQ(t1.temp,t2.temp)&IQ(t1.tempType,t2.tempType)']
x=dcparser.DCParser(dcCode)
and_of_preds=x.make_and_condition('all')
dce=dcerrordetector.DCErrorDetection(dcCode,d)

violation = dce.proxy_violated_tuples()
prox=violation[0]

for i in prox:
    print(i)

# hs=holocleansession.HolocleanSession(d,'local')
# 
# x=d.get_table("T")
# y=hs._covert2_spark_dataframe('T')
# print(x)
# print(y.show())
# print(y.toPandas())
