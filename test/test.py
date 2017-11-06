from holoclean.holoclean import HoloClean,Session
a=HoloClean()
b=Session("Session",a)
b.ingest_dataset("10.csv")
