import sys
import os
sys.path.append('../')
from holoclean import holocleansession , dataset , dataengine
from holoclean.errordetection import dcerrordetector
from holoclean.utils import dcparser


my_path = os.path.abspath(os.path.dirname(__file__))
path = os.path.join(my_path, "../holoclean/mysql-connector-java-5.1.44-bin.jar")
holoclean_se=holocleansession.HolocleanSession(path)
#spark_session=holoclean_se._start_spark_session(path)
ds=dataset.Dataset()  
d=dataengine.Dataengine("metadb-config.txt",'datadb-config.txt',ds,holoclean_se.return_sqlcontext())
holoclean_se.set_dataengine(d)
df=d.ingest_spark('10.csv',holoclean_se.returnspark_session())

df.show()


dcCode=['t1&t2&EQ(t1.city,t2.city)&EQ(t1.temp,t2.temp)&IQ(t1.tempType,t2.tempType)']

dk_cells,clean_cells=holoclean_se._error_detection(dcCode)
dk_cells.show()

dk_cells_in_db=d.get_table_spark("C_dk")
dk_cells_in_db.show()




