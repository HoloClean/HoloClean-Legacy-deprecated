from holoclean.holoclean import HoloClean,Session
from holoclean.errordetection.errordetector import ErrorDetectors
from holoclean.featurization.featurizer import Signal_Init,Signal_cooccur,Signal_dc
from holoclean.dataset import Dataset

class Intermed:

    def get_dataset(self,pathfile):
        reader = open(pathfile, 'r')
        id = reader.readlines()[0]

        ds = Dataset()
        ds.dataset_tables_specific_name[0] = id[:-1]
        for li in range(1, len(ds.dataset_tables_specific_name)):
            ds.dataset_tables_specific_name[li] = str(id[:-1]) + "_" + str(ds.attributes[li])

        return ds

    def set_dataset(self,pathfile, dataset_id):
        writer = open(pathfile, 'w')
        writer.writelines(dataset_id)

I=Intermed()
#import reader as dcreader
a=HoloClean()
b=Session("Session",a)

ds=I.get_dataset("abc.txt")
print (ds.dataset_tables_specific_name)
b.dataset=ds
print b.dataset.dataset_id
b._wrapper()
