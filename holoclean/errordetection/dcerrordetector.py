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

    def index2list(self,dataset):        
        """
        Returns list of indices
        :rtype: list[string]
        """
        li_tmp=dataset.select('index').collect()
        
        return [i.asDict()['index'] for i in li_tmp ]

    def featurize(self,noviolations,dataset):
        temp_dataset=dataset
        num_of_rows=temp_dataset.count()*(len(temp_dataset.columns)-1)
        data=[]
	#noviolations[0].show()
	#dataset.show()
	#raw_input("input!!!")
        indexCol=self.index2list(dataset)
	#print indexCol
	#print temp_dataset.columns
	#raw_input("input2!!!")
        for row in indexCol:
	  #  print row
            for p in temp_dataset.columns:
	#	print p
                if p!='index':
                    tm=[-1]*(temp_dataset.count()*len(self.and_of_preds))
                    data.append([(row, p)]+tm)
	#print data
	#raw_input("input2!!!")

        for dc_count in range(0,len(self.and_of_preds)):
            tmp=noviolations[dc_count] 
	   # tmp.show()
	   # print dc_count
	   # raw_input("asdasddasdasda")          
            for i in tmp.collect():
                row_tuple=i.asDict()['ind']
                col_tuple=i.asDict()['indexT2']
                for tu_count in range(0,num_of_rows):
                    cell_info =data[tu_count][0]
                    curr_tuple_index=cell_info[0]
                    curr_tuple_attribute=cell_info[1]

                    col_changed=(dc_count) * dataset.count() + indexCol.index(col_tuple) + 1
                    if self.inclusion(curr_tuple_attribute,dc_count):                       
                        if int(curr_tuple_index) == int(row_tuple):
                            data[tu_count][col_changed] = 1
                    else:
                        data[tu_count][col_changed] = 0

        col_names=['cell']+[str(i) for i in range(1,temp_dataset.count()*len(self.and_of_preds)+1)]

        new_df = self.spark_session.createDataFrame(data,col_names)
        
        return(new_df) 
 
    def inclusion (self, attribute, denial_index):
        #This function by getting denial_index return TRUE if attribute
        # included in the denial constraints that index in denial_constraint
        if attribute in self.and_of_preds[denial_index]:
            return True
        else:
            return False
    
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



