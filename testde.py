from holoclean import holoclean


query = " select rv_attr,feature from 653521713966_Feature LIMIT 10000000"
hc = holoclean.HoloClean()
df = hc.dataengine.query(query,1)

list = df.collect()

