import sys
import os
sys.path.append('../')
from holoclean import holocleansession , dataset , dataengine
from holoclean.errordetection import dcerrordetector
from holoclean.utils import dcparser


my_path = os.path.abspath(os.path.dirname(__file__))
path = os.path.join(my_path, "../holoclean/mysql-connector-java-5.1.44/mysql-connector-java-5.1.44-bin.jar")
holoclean_se=holocleansession.HolocleanSession(path)
#spark_session=holoclean_se._start_spark_session(path)
ds=dataset.Dataset()  
d=dataengine.Dataengine("metadb-config.txt",'datadb-config.txt',ds,holoclean_se.return_sqlcontext())
d.ingest_spark('10.csv',holoclean_se.returnspark_session())
#sql_query="select * from 81789958447_T"
#df=d.retrieve_spark(sql_query,holoclean_se.return_sqlcontext())
df=d.get_table_spark("T")
df.show()
#d.ingest_cursor('10.csv')
#dd=d.register("C_clean","ind,attr")


#dcCode=['t1&t2&EQ(t1.city,t2.city)&EQ(t1.temp,t2.temp)&IQ(t1.tempType,t2.tempType)']
#x=dcparser.DCParser(dcCode)
#and_of_preds=x.make_and_condition('all')
#df=d.get_schema("T").split(',')
#attributes=dcparser.DCParser.get_attribute(and_of_preds[0],df)
#print attributes
#d.attributes(attributes)
#dce=dcerrordetector.DCErrorDetection(dcCode,d)


#violation = dce.proxy_violated_tuples()
#prox=violation[0]

#for i in prox:
 #  print(i[0])

# hs=holocleansession.HolocleanSession(d,'local')
#
# x=d.get_table("T")
# y=hs._covert2_spark_dataframe('T')
# print(x)
# print(y.show())
# print(y.toPandas())
# import sys
# sys.path.append('../')
# from holoclean import holocleansession , dataset , dataengine
# from holoclean.errordetection import dcerrordetector
# from holoclean.utils import dcparser
#
#
# ds=dataset.Dataset()  
# d=dataengine.Dataengine("metadb-config.txt",'datadb-config.txt',ds)
# d.ingest('10.csv')
# dd=d.register("C_clean","ind,attr")
#
#
# dcCode=['t1&t2&EQ(t1.city,t2.city)&EQ(t1.temp,t2.temp)&IQ(t1.tempType,t2.tempType)']
# x=dcparser.DCParser(dcCode)
# and_of_preds=x.make_and_condition('all')
# dce=dcerrordetector.DCErrorDetection(dcCode,d)
#
# violation = dce.proxy_violated_tuples()
# prox=violation[0]
#
#
# for i in prox:
#     print(i)
#
# # hs=holocleansession.HolocleanSession(d,'local')
# #
# # x=d.get_table("T")
# # y=hs._covert2_spark_dataframe('T')
# # print(x)
# # print(y.show())
# # print(y.toPandas())


