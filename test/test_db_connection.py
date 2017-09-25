import unittest
import sys
sys.path.append('../')
from holoclean import dataengine , dataset 
from holoclean.utils import ingest


class Dbtest(unittest.TestCase):
    
    
    ds=dataset.Dataset()   
    d=dataengine.Dataengine("metadb-config.txt",'datadb-config.txt',ds)
    a=ingest.Ingest("10.csv")
    a.reader(d) 
    meta_schema=d.get_schema("T")
    sql_query="Select * from "+ds.table_name[1]
    df=d.retrieve(sql_query)
    size_of_retrived_data=len(df.index)
    
    
    
    def test_register_dataset(self):
        self.assertIsNotNone(self.ds,"The dataset object created successfully")        
        self.assertEqual(self.d.meta_filepath, "metadb-config.txt","The dataset object created successfully")        
        print("ok")
        
    def test_retrive_data(self):

        self.assertEqual(self.meta_schema, "index,city,date,tempType,temp")
        self.assertEqual(self.size_of_retrived_data, 10)

if __name__ == '__main__':
    unittest.main()


