import sys
sys.path.append('../')
from holoclean import holocleansession , dataset , dataengine


ds=dataset.Dataset()   
d=dataengine.Dataengine("metadb-config.txt",'datadb-config.txt',ds)
hs=holocleansession.HolocleanSession(d,'local')

