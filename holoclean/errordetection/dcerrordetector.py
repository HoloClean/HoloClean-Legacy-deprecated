import sys 
sys.path.append('../../')
from holoclean.utils import dcparser

class DCErrorDetection:
    
    
    def __init__(self,DenialConstraints,dataengine):
        self.and_of_preds=dcparser.DCParser(DenialConstraints).make_and_condition('all')
        self.dataengine=dataengine

    def fill_tables(self):
        pass
        

            
    def proxy_violated_tuples(self):
        
        T_table_name=self.dataengine.dataset.spec_tb_name('T')
        attr_set = self.dataengine.get_schema('T').split(',')
        
        violates=[]
        
        for cond in  self.and_of_preds:
            
            q="SELECT table1.index as indexT1 ,table2.index as indexT2 FROM " + T_table_name + " table1,"+T_table_name+ " table2 WHERE ("+cond+")"
            
            violates.append(self.dataengine.query(q))
            
        return violates
            
            
            
            
        
    
    
    
    
    
    ####### Spark Methods #############
    
    def no_violation_tuples(self,spark_session):
        
        dataset.createOrReplaceTempView("df") 
        satisfied_tuples_index=[]
        for cond in  self.and_of_preds: 
            q="SELECT table1.index as indexT1,table2.index as indexT2 FROM df table1,df table2 WHERE NOT("+cond+")"        
            satisfied_tuples_index.append(spark_session.sql(q))         
        return satisfied_tuples_index
    
    def violation_tuples(self,spark_session):
        
        dataset.createOrReplaceTempView("df") 
        satisfied_tuples_index=[]
        for cond in  self.and_of_preds: 
            q="SELECT table1.index as indexT1,table2.index as indexT2 FROM df table1,df table2 WHERE ("+cond+")"        
            satisfied_tuples_index.append(spark_session.sql(q))         
        return satisfied_tuples_index



