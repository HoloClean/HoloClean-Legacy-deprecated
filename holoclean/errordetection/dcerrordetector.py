import sys 
sys.path.append('../../')
from holoclean.utils import dcparser

class DCErrorDetection:
    
    def __init__(self,DenialConstraints,dataengine,spark_session):
        self.and_of_preds=dcparser.DCParser(DenialConstraints).make_and_condition('all')
        self.dataengine=dataengine
        self.spark_session=spark_session
    
    def no_violation_tuples(self,dataset):
        
        dataset.createOrReplaceTempView("df")   
        satisfied_tuples_index=[]
        for cond in  self.and_of_preds: 
            q="SELECT table1.index as ind,table2.index as indexT2 FROM df table1,df table2 WHERE NOT("+cond+")"        
            satisfied_tuples_index.append(self.spark_session.sql(q))         
        return satisfied_tuples_index
    
    def violation_tuples(self,dataset):
        
        dataset.createOrReplaceTempView("df") 
        satisfied_tuples_index=[]
        for cond in  self.and_of_preds: 
            q="SELECT table1.index as ind,table2.index as indexT2 FROM df table1,df table2 WHERE ("+cond+")"        
            satisfied_tuples_index.append(self.spark_session.sql(q))         
        return satisfied_tuples_index
    
    
    def make_cells(self,tuples_dataframe,cond):
        all_list=self.dataengine.get_schema("T")
        all_list=all_list.split(',')
        attr_list=dcparser.DCParser.get_attribute(cond,all_list)
        index_data=tuples_dataframe.select('ind').unionAll(tuples_dataframe.select('indexT2')).distinct()
        dc_data=[]
        for i in attr_list:
            dc_data.append([i])
        dc_df = self.spark_session.createDataFrame(dc_data,['attr'])
        
        result = index_data.crossJoin(dc_df)
        return result
    
    def noisy_cells(self,dataset):
        num_of_constarints= len(self.and_of_preds)
        violation = self.violation_tuples(dataset)
        result=self.make_cells(violation[0],self.and_of_preds[0])
        if num_of_constarints>1:
            for i in range(1,num_of_constarints):
                result=result.unionAll(self.make_cells(violation[i], self.and_of_preds[i]))
        return result.distinct()
    
    def clean_cells(self,dataset,noisy_cells):
#         index_set=dataset.select['index']
        dataset.createOrReplaceTempView("df")
        q="SELECT table1.index as ind FROM df table1"
        index_set=self.spark_session.sql(q)
        all_attr=self.dataengine.get_schema("T").split(',')
        rev_attr_list=[]
        for i in all_attr:
            rev_attr_list.append([i])
        all_attr_df = self.spark_session.createDataFrame(rev_attr_list,['attr'])
        all_cell=index_set.crossJoin(all_attr_df)
        
        result =all_cell.subtract(noisy_cells) 
        return result



