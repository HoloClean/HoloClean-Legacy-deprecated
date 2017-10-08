import sys
from distributed.diagnostics.progress_stream import counter
sys.path.append('../../')
from holoclean.utils import dcparser
import pyspark as ps
from numpy import unique
import sqlalchemy as sa

class DCFeaturizer:
    
    #########################################################
    ################# Initialize ############################ 
    #########################################################
    
    #Set of operations that can appear in the denial constraints 

    
    #For each QuantativeStatisticsFeaturize , data_dataframe and noisy cells are needed
    def __init__(self,data_dataframe,denial_constraints,dataengine):
        self.data_dataframe=data_dataframe
        self.denial_constraints=denial_constraints
        self.dataengine=dataengine
 
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
   
    #########################################################
    ########## Make feature for multiple tuples ############# 
    #########################################################
    
    #@@@@@@@@ Main Method @@@@@@@@@@#
    
    def featurize(self,noviolations,spark_session):
        temp_dataset=self.data_dataframe
#         num_of_rows=temp_dataset.count()*(len(temp_dataset.columns)-1)-len(self.noisy_cells)
        num_of_rows=temp_dataset.count()*(len(temp_dataset.columns)-1)
        data=[]
    
        #######Creating default data
        indexCol=self.index2list()
        for row in indexCol:
            for p in temp_dataset.columns:
#                 if self.not_noisy(row, p) and p!='index':
                if p!='index':
                    tm=[-1]*(temp_dataset.count()*len(self.denial_constraints))
                    data.append([(row, p)]+tm)
        ##########################################

        for dc_count in range(0,len(self.denial_constraints)):
            tmp=noviolations[dc_count]           
            #GO over truth value
            for i in tmp.collect():
                row_tuple=i.asDict()['indexT1']
                col_tuple=i.asDict()['indexT2']
                for tu_count in range(0,num_of_rows):
                    cell_info =data[tu_count][0]
                    curr_tuple_index=cell_info[0]
                    curr_tuple_attribute=cell_info[1]
                    
                    #Change the data arrays
                    col_changed=(dc_count) * self.data_dataframe.count() + indexCol.index(col_tuple) + 1
                    if self.inclusion(curr_tuple_attribute,dc_count):                       
                        if int(curr_tuple_index) == int(row_tuple):
                            data[tu_count][col_changed] = 1
                    else:
                        data[tu_count][col_changed] = 0

        col_names=['cell']+[str(i) for i in range(1,temp_dataset.count()*len(self.denial_constraints)+1)]

        new_df = spark_session.createDataFrame(data,col_names)
        
        return(new_df)  
   
    
    #$$$$$$$$$$$$$$$$ Not Main $$$$$$$$$$$$$$
    
    
    def pre_features(self,spark_session):
        data=[]
        index_id=self.index2list()
        for i in index_id:
            for a in self.data_dataframe.columns:
                if a!='index':
                    act_dom=self.attribute_active_domain(a)
                    for val in act_dom:
                        for dc in self.denial_constraints:
                            for j in index_id:
                                if self.violate(i,a,val,j,dc,spark_session):
                                    data.append([str('t_'+i+'.'+a),val,dc,j])
        col_names=['rv','assigned','dc','tup_id']                            
        new_df = spark_session.createDataFrame(data,col_names)
        
        return new_df
    
    """Supplementary methods"""
    
    def violate(self,cell_index,cell_attr,value,second_tuple_index,dc,spark_session):
        if cell_attr not in dc :
            return False
        else:
            dc_index=self.denial_constraints.index(dc)
            dcp=dcparser.DCParser(self.denial_constraints)
            dcSql,usedOperations=dcp.dc2SqlCondition()
                   
            standard_dc=dcSql[dc_index]
            op_dc=usedOperations[dc_index]
            self.data_dataframe.createOrReplaceTempView("df")
            q="SELECT * FROM df WHERE( index = '"+str(cell_index) +"' OR index='"+str(second_tuple_index)+"')"
            df_q_result= spark_session.sql(q)
            rows=df_q_result.collect()
            new_dic=[]
            for r in rows :
                tmp=r.asDict()
                if tmp['index']==str(cell_index):
                    tmp[cell_attr]=value
                new_dic.append(tmp)
            new_df = spark_session.createDataFrame(new_dic)
            new_df.createOrReplaceTempView("dfn")
            sql_q=dcp.make_and_condition(conditionInd = 'all')[dc_index]
            q="SELECT table1.index as indexT1,table2.index as indexT2 FROM dfn table1,dfn table2 WHERE ("+ sql_q +")"
            
            result=spark_session.sql(q)
            if result.count()== 0:
                return False
            else:
    
                return True
       
    
    def make_dc_f_table(self):
        view_name=self.make_pre_feature()
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        create_table='CREATE TABLE '+self.dataengine.dataset.spec_tb_name('dc_f_mysql')+' AS SELECT * FROM '+view_name+';'
        cursor.execute(create_table)        
        
            
    def make_pre_feature(self):
        table_name,union_view=self.make_union_view()
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        db_name=self.dataengine.dbname
        view_name=db_name+'.pre_feature_'+table_name
        feature_view='CREATE VIEW '+view_name+' AS SELECT rv_index,rv_attr,assigned_val,DCname,tup_id FROM '+union_view+' WHERE (rv_index<>tup_id)'
        cursor.execute(feature_view)      
        return view_name
        
    def make_union_view(self):
        table_name,cond_view_name=self.make_dc_view()
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        db_name=self.dataengine.dbname
        view_name=db_name+'.union_view_'+table_name
        feature_view='CREATE VIEW '+view_name+' AS '
        for cond in cond_view_name:
            feature_view+='SELECT * from '+cond[1]+' UNION '
        feature_view=feature_view[:-6]
        feature_view+='GROUP BY rv_index,rv_attr,assigned_val,DCname,tup_id;'
        cursor.execute(feature_view)

        return table_name,view_name
    
    def make_dc_view(self):
        table_name,prod_view_name=self.make_cross_veiw()
        dcp=dcparser.DCParser(self.denial_constraints)
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        db_name=self.dataengine.dbname
#         dc_sql_parts=self.make_and_list(dc_sql_parts)
        #select city,IF(city IS NOT NULL,'Gooood','bad') as name from holocleandb.prod_view_tempType_0666684774209_T
        cond_view_name=[]
        for prod_view in prod_view_name:
            counter=1
            for dcs in dc_sql_parts:
                check_dc_query='CREATE VIEW '+db_name+'.cond_view_'+prod_view[0]+'_'+table_name+'_dc'+str(counter)+' AS SELECT table1.index2 as rv_index,IF(table1.index2 IS NOT NULL,"'+prod_view[0]+'","Bad") as rv_attr,table1.'+prod_view[0]+' as assigned_val,IF(table1.index2 IS NOT NULL,"'+dcs+'","No dc") as DCname,table2.index as tup_id  FROM '+prod_view[1]+' table1,'+table_name+' table2 WHERE ('+dcs+') GROUP BY rv_index,rv_attr,assigned_val,DCname,tup_id;'
                cond_view_name.append([prod_view[0],db_name+'.cond_view_'+prod_view[0]+'_'+table_name+'_dc'+str(counter)])
                cursor.execute(check_dc_query)
                counter+=1
        return table_name,cond_view_name
                      
    def make_cross_veiw(self):
        table_name,view_names=self.create_views()
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        db_name=self.dataengine.dbname
        prod_view_name=[] 
        for view_pair in view_names:
            cross_join_query='CREATE VIEW '+db_name+'.prod_view_'+view_pair[0]+'_'+table_name+' AS SELECT *  FROM '+view_pair[1]+' CROSS JOIN '+view_pair[2]+';'     
            cursor.execute(cross_join_query)
            prod_view_name.append([view_pair[0],db_name+'.prod_view_'+view_pair[0]+'_'+table_name])
        
        return table_name,prod_view_name
    
    def create_views(self):
        
        db_name=self.dataengine.dbname
        table_name=self.dataengine.dataset.spec_tb_name('T')
        table_attribute_string=self.dataengine.get_schema('T')
        table_attribute=table_attribute_string.split(',')
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        print (table_name)
        view_names=[]
        for attr in table_attribute:
            if attr != 'index':
                attr_alone_view='CREATE VIEW '+db_name+'.'+attr+'_col_'+table_name+' AS SELECT '+table_name+'.index as index1,'+attr+' FROM '+table_name+';'
                rest_cols_view='CREATE VIEW '+db_name+'.'+attr+'_mis_'+table_name+' AS SELECT '+table_name+'.index as index2,'+self.make_list_drop(table_attribute, attr)+' FROM '+table_name+';'
                tmp=[attr,db_name+'.'+attr+'_col_'+table_name,db_name+'.'+attr+'_mis_'+table_name]
                view_names.append(tmp)
                cursor.execute(attr_alone_view)
                cursor.execute(rest_cols_view)
        print ("OK!")
        return table_name,view_names
    
    def create_pre_featurize(self):
        dcp=dcparser.DCParser(self.denial_constraints)
        possib_view=self.create_possible_value()
        db_name=self.dataengine.dbname
        table_name=self.dataengine.dataset.spec_tb_name('T')
        dc_list=self.create_relaxed_dc()
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')

        check_dc_query='CREATE VIEW '+db_name+'.result_view_'+'_'+table_name+' AS'        
        for cnt in range(len(dc_sql_parts)):
            for rel in dc_list[cnt]:
                check_dc_query+=' SELECT table3.tid as rv_index,table3.attr_name as rv_attr,table3.attr_val as assigned_val,IF(table3.tid IS NOT NULL,"'+dc_sql_parts[cnt]+'","No dc") as DCname,'+rel[0]+'.index as tup_id FROM '+possib_view+' table3,'+table_name+' table1,'+table_name+' table2 WHERE ('+rel[1]+') UNION'
        
        check_dc_query=check_dc_query[:-5]
        check_dc_query+='GROUP BY rv_index,rv_attr,assigned_val,DCname,tup_id;'
#         cursor.execute(check_dc_query)
        print(check_dc_query)

    def create_relaxed_dc(self):
        dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        relaxed_dcs=[]
        for dc_sql in dc_sql_parts:
            tmp_list=[]
            preds=dc_sql.split('AND')
            num_of_preds=len(preds)
            for i in range(num_of_preds):
                if 'table1' in preds[i]:
                    tmp=preds[i].replace('table1','table3')
                    tmp_list.append(['table2',dc_sql.replace(preds[i],tmp)])
                if 'table2' in preds[i]:
                    tmp=preds[i].replace('table2','table3')
                    tmp_list.append(['table1',dc_sql.replace(preds[i],tmp)])
            relaxed_dcs.append(tmp_list)
        return relaxed_dcs
    
    def make_new_cond(self,cond):
        list_preds=cond.split(' AND ')
        new_cond=''
        for pred in list_preds:
            new_cond+=self.create_new_pred(pred)+' AND '
        new_cond=new_cond[:-5]
        new_pred_parts=new_cond.split(' AND ')
        tmp=list(unique(new_pred_parts))
        result=''
        for e in tmp:
            result+=e+' AND '
        result=result[:-5]
        return result
        
        
        
    
    def create_new_pred(self,old_pred):
        dcp=dcparser.DCParser(self.denial_constraints)
        op_set=dcp.operationsArr
        op_set=sorted(op_set, key=len, reverse=True)
        for oper in op_set:
            if oper in old_pred:
                pred_parts=old_pred.split(oper)
                first_table=pred_parts[0].split('.')[0]
                first_attr=pred_parts[0].split('.')[1]
                second_table=pred_parts[1].split('.')[0]
                second_attr=pred_parts[1].split('.')[1]
                if second_attr.replace(' ','') != first_attr.replace(' ',''):
                    new_pred=first_table+'.attr_name='+first_attr+' AND '+second_table+'.attr_name='+second_attr+' AND '+old_pred.replace(first_attr,'attr_val').replace(second_attr,'attr_val')
                else:
                    new_pred=first_table+'.attr_name='+second_table+'.attr_name AND '+old_pred.replace(first_attr,'attr_val').replace(second_attr,'attr_val')
                return new_pred
        
    
               
    def create_possible_value(self):
        
        db_name=self.dataengine.dbname
        table_name=self.dataengine.dataset.spec_tb_name('T')
        table_attribute_string=self.dataengine.get_schema('T')
        table_attribute=table_attribute_string.split(',')
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        view_names=[]
        cell_possible_view='CREATE VIEW '+db_name+'.value_possible_'+table_name+' AS'
        instance1=table_name+'1'
        instance2=table_name+'2'
        index_name=instance1+'.index'
        for attr in table_attribute:
            if attr != 'index':
                cell_possible_view+=' SELECT '+index_name+' as tid,IF('+index_name+' IS NOT NULL,"'+attr+'","Bad") as attr_name,'+instance2+'.'+attr+' as attr_val FROM '+table_name+' '+instance1+','+table_name+' '+instance2+' UNION '
        cell_possible_view=cell_possible_view[:-6]
        cell_possible_view+=';'
        cursor.execute(cell_possible_view)
        
        return db_name+'.value_possible_'+table_name  
    
    def create_init_value(self):
        
        db_name=self.dataengine.dbname
        table_name=self.dataengine.dataset.spec_tb_name('T')
        table_attribute_string=self.dataengine.get_schema('T')
        table_attribute=table_attribute_string.split(',')
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        view_names=[]
        cell_possible_view='CREATE VIEW '+db_name+'.value_init_'+table_name+' AS'

        index_name=table_name+'.index'
        for attr in table_attribute:
            if attr != 'index':
                cell_possible_view+=' SELECT '+index_name+' as tid,IF('+index_name+' IS NOT NULL,"'+attr+'","Bad") as attr_name,'+attr+' as attr_val FROM '+table_name+' UNION '
        cell_possible_view=cell_possible_view[:-6]
        cell_possible_view+=';'
#         cursor.execute(cell_possible_view)
        print(cell_possible_view)
        return db_name+'.value_init_'+table_name    
    
    def make_list_drop(self,org_list,attr):
        result=''
        for i in org_list:
            if i != attr and i != 'index':
                result+=i+','
        return result[:-1]

    def make_and_list(self,dc_sql_parts):
        dc_sql_string=[]
        for q_l in dc_sql_parts:
            tmp=''
            for i in q_l:
                tmp+=i +' AND '
            dc_sql_string.append(tmp[:-5])
        return dc_sql_string





                         
    
    def attribute_active_domain(self,attribute):
            
        """
        Returns the full domain of an attribute
        :type attr: string
        :rtype: list[string]
        """
        domain = set()
        tmp=self.data_dataframe.select(attribute).collect()
        for v in range(0,len(tmp)):
            domain.add(tmp[v].asDict()[attribute])
        return list(domain) 
    
    
    def not_noisy(self, index , attribute,noisy_cells):
        # This function get some index and attribute and by considering the noisy cell arrays return the true if it is not noisy
        if (index,attribute) in noisy_cells:
            return False
        return True
    
    
    
    
    #This function check that this objects attribute is included in denial constraint we need it in making denial constraint put zero in place
    
    
    def inclusion (self, attribute, denial_index):
        #This function by getting denial_index return TRUE if attribute
        # included in the denial constraints that index in denial_constraint
        if attribute in self.denial_constraints[denial_index]:
            return True
        else:
            return False
    
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@   
    
    #########################################################
    ########### Make feature vector for one tuple ########### 
    #########################################################
    
    # This function make feature vector for a cell with index i  j in the data_dataframe
    
    
    def make_featurvector(self, index, attribute ,parsedDCs,spak_session):
        numOfTuple = self.data_dataframe.count()
        result =[0]*int(len(self.denial_constraints)*numOfTuple + 1)
        result[0] = (index , attribute)
        cc = 1
        indices=self.index2list()
        for dccount in range(0,len(parsedDCs)):
            for p in indices:
                if self.inclusion(attribute,dccount):
                    result[cc]=self.truthEval(parsedDCs[dccount], index, p, spak_session)
                else:
                    result[cc]=0
                cc+=1
        return result
       
    
    def truthEval(self,parsedCode,indexTuple1,indexTuple2,spak_session):
        #'table1.city=table2.city', 'table1.temp=table2.temp', 'table1.tempType<>table2.tempType'
        """for example we can consider t1.a1!=t2.a1 and t1.a2=t2.a2
        its code is something like a string "a1,3,a2,0"
        """
        dfLeft=self.data_dataframe.filter(self.data_dataframe['index']==indexTuple1)
        dfRight=self.data_dataframe.filter(self.data_dataframe['index']==indexTuple2)
        dfLeft.createOrReplaceTempView("dfl")
        dfRight.createOrReplaceTempView("dfr")
        
        
        trueCombination=[]
        for i in range(0,len(parsedCode)):       
            q="SELECT table1.index as indexT1,table2.index as indexT2 FROM dfl table1,dfr table2 WHERE NOT("+ parsedCode[i]+")"
            trueCombination.append(spak_session.sql(q))
        satisfied_tuples_index=trueCombination[0]
        for i in range(0,len(parsedCode)):
            satisfied_tuples_index=satisfied_tuples_index.union(trueCombination[i])        
        if satisfied_tuples_index.count()>0 :
            return 1
        return -1
        
    
    def index2list(self):        
        """
        Returns list of indices
        :rtype: list[string]
        """
        li_tmp=self.data_dataframe.select('index').collect()
        
        return [i.asDict()['index'] for i in li_tmp ]

    
    def indexValue(self,ind,attr):

        """
        Returns the value of cell with index=ind and column=attr
        :type ind: int
        :type attr: string
        :rtype: list[list[None]]
        """
        row=self.data_dataframe.filter(self.data_dataframe['index']==ind).collect()
        row2dict=row[0].asDict()
        
        return row2dict[attr]    
    
    
  
    