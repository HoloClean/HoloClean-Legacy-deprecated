import sys
import os
sys.path.append('../')
from holoclean import holocleansession , dataset , dataengine
from holoclean.utils import domainpruning
from holoclean.featurization import dcfeaturizer
import time




my_path = os.path.abspath(os.path.dirname(__file__))
path = os.path.join(my_path, "../holoclean/mysql-connector-java-5.1.44-bin.jar")
holoclean_se=holocleansession.HolocleanSession(path)

ds=dataset.Dataset()  
d=dataengine.Dataengine("metadb-config.txt",'datadb-config.txt',ds,holoclean_se.return_sqlcontext())
holoclean_se.set_dataengine(d)
df=d.ingest_spark('10.csv',holoclean_se.returnspark_session())


dcCode=['t1&t2&EQ(t1.city,t2.city)&EQ(t1.temp,t2.temp)&IQ(t1.tempType,t2.tempType)']

# dk_cells,clean_cells=holoclean_se._error_detection(dcCode,df)

dcf=dcfeaturizer.DCFeaturizer(df,dcCode,d)

# dgf=dcf.pre_features(holoclean_se.returnspark_session())
# dgf.show()
# table_name,view_names=dcf.create_views()
start_time_mysql = time.time()
dcf.make_dc_f_table()
finish_time_mysql=time.time()
mysqlv=d.get_table_spark("dc_f")
print(mysqlv.show())

print("--- %s seconds ---" % (finish_time_spark-start_time_spark))



