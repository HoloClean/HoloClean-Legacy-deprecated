from holoclean.holoclean import HoloClean,Session
from holoclean.errordetection.errordetector import ErrorDetectors
from holoclean.featurization.featurizer import Signal_Init,Signal_cooccur,Signal_dc
from holoclean.dataset import Dataset

class Intermed:

    def get_dataset(self,pathfile):
        reader = open(pathfile, 'r')
        id = reader.readlines()[0]

        ds = Dataset()
        ds.dataset_tables_specific_name[0] = id
	ds.dataset_id=id
        for li in range(1, len(ds.dataset_tables_specific_name)):
            ds.dataset_tables_specific_name[li] = str(id) + "_" + str(ds.attributes[li])

        return ds

    def set_dataset(self,pathfile, dataset_id):
        writer = open(pathfile, 'w')
        writer.writelines(dataset_id)

I=Intermed()
#import reader as dcreader
a=HoloClean()
b=Session("Session",a)

ds=I.get_dataset("abc.txt")

b.dataset=ds

b._numskull()
