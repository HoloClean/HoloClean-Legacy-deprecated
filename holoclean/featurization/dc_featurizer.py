import sys
#from distributed.diagnostics.progress_stream import counter
sys.path.append('../../')
from holoclean.utils import dcparser
import pyspark as ps
from numpy import unique
import sqlalchemy as sa
import itertools
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import when  

class DCFeaturizer:
    
    def __init__(self,data_dataframe,denial_constraints,dataengine):
        self.data_dataframe=data_dataframe
        self.denial_constraints=denial_constraints
        self.dataengine=dataengine
    
    def create_possible_table_value(self):
        
        db_name=self.dataengine.dbname
        table_name=self.dataengine.dataset.spec_tb_name('T')
        table_attribute_string=self.dataengine.get_schema('T')
        table_attribute=table_attribute_string.split(',')
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        view_names=[]
        cell_possible_view='CREATE TABLE '+db_name+'.value_possible1_'+table_name+' AS'
        instance1=table_name+'1'
        instance2=table_name+'2'
        index_name=instance1+'.index'
        for attr in table_attribute:
            if attr != 'index':
                cell_possible_view+=' SELECT '+index_name+' as tid,IF('+index_name+' IS NOT NULL,"'+attr+'","Bad") as attr_name,'+instance2+'.'+attr+' as attr_val FROM '+table_name+' '+instance1+','+table_name+' '+instance2+' UNION '
        cell_possible_view=cell_possible_view[:-6]
        cell_possible_view+=';'
        cursor.execute(cell_possible_view)

	return db_name+'.value_possible1_'+table_name
   
    def table_featurizer(self):
	db_name=self.dataengine.dbname
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        view_names=[]
	possible_name=self.create_possible_table_value()
	table_name=self.dataengine.dataset.spec_tb_name('T')
	dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
	final1=self.change_cond()

	for p in range (0, len(dc_sql_parts)):
		final=final1[p]
		final_number=final[0]
		final_condition=final[1]
		dc="'"+dc_sql_parts[0]+"'"
		table_feaurizer='CREATE TABLE '+self.dataengine.dataset.spec_tb_name('dc_f1')+' AS '
		table_feaurizer+="""SELECT distinct    table1.index as rv_index, table3.attr_val as assigned_val, table3.attr_name as rv_attr, table2.index as tup_id, IF(table3.tid IS NOT NULL, """+dc+""" ,"No dc") as DC_name  from """+ table_name +""" as table2, """+ table_name+ """ as table1, """+ possible_name+ """ as table3 where ("""+final_condition+""" AND (table1.index<>table2.index) ) order BY rv_index,rv_attr,assigned_val,DC_name,tup_id;"""
		cursor.execute(table_feaurizer)

	return table_feaurizer

    def table_featurizer_pruning(self):
	db_name=self.dataengine.dbname
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        view_names=[]
	table_name=self.dataengine.dataset.spec_tb_name('T')
	possible_name=self.dataengine.dataset.spec_tb_name('D')
	dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
	final1=self.change_condition()

	for p in range (0, len(dc_sql_parts)):
		final=final1[p]
		final_number=final[0]
		final_condition=final[1]
		dc="'"+dc_sql_parts[0]+"'"
		table_feaurizer='CREATE TABLE '+self.dataengine.dataset.spec_tb_name('dc_f1')+' AS '
		table_feaurizer+="""SELECT  distinct  table1.index as rv_index, table3.value as assigned_val, table3. attr as rv_attr, table2.index as tup_id, IF(table1.index IS NOT NULL, """+dc+""" ,"No dc") as DC_name  from """+ table_name +""" as table2, """+ table_name+ """ as table1, """+ possible_name+ """ as table3 where ("""+final_condition+""" );"""
		cursor.execute(table_feaurizer)
	return table_feaurizer


    def change_condition(self):
	table_attribute_string=self.dataengine.get_schema('T')
       	d=table_attribute_string.split(',')
	dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        new_dcs=[]
	i=0
        for c in dc_sql_parts:
		list_preds,type_list=self.table_type(c)
		new_dcs.append(self.table_type_new_cond(list_preds,d,i))
	return new_dcs

    def change_cond(self):
	table_attribute_string=self.dataengine.get_schema('T')
       	d=table_attribute_string.split(',')
	dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        new_dcs=[]
	i=0
        for c in dc_sql_parts:
		list_preds,type_list=self.table_type(c)
		new_dcs.append(self.table_type_cond(list_preds,d,i))
	return new_dcs

    
    def table_type_cond(self,cond,d,number):
	operationsArr=['<>' , '<=' ,'>=','=' , '<' , '>']
	type_list=[]
	for i in range(0,len(cond)):
		list_preds=cond[i].split('.')
		string1=""
		for p in (0,len(list_preds)-1):
			if list_preds[p] in d:
				for text in operationsArr:
					if text in list_preds[p-1] :
						list3=list_preds[p-1].split(text)
						string1="table3.attr_name= '"+list_preds[p]+"' AND " + "table3.attr_val"+text+list3[1]+"."+list_preds[p]
						break
				for k in range(0,len(cond)):
					if k!=i:
						string1=string1+" AND "+cond[k]
				type_list.append([string1])
				break
	final=""

	final=final+"("+type_list[0][0]+")"
	for i in (1,len(type_list)-1):
		final=final+" OR "+"("+type_list[i][0]+")"
	final1=[number,final]
	return final1
 
    def table_type_new_cond(self,cond,d,number):
	operationsArr=['<>' , '<=' ,'>=','=' , '<' , '>']
	type_list=[]
	for i in range(0,len(cond)):
		list_preds=cond[i].split('.')
		string1=""
		for p in (0,len(list_preds)-1):
			if list_preds[p] in d:
				for text in operationsArr:
					if text in list_preds[p-1] :
						list3=list_preds[p-1].split(text)
						string1="table3.attr= '"+list_preds[p]+"' AND " + "table3.value"+text+list3[1]+"."+list_preds[p]
						break
				for k in range(0,len(cond)):
					if k!=i:
						string1=string1+" AND "+cond[k]
				type_list.append([string1])
				break
	final=""

	final=final+"("+type_list[0][0]+")"
	for i in (1,len(type_list)-1):
		final=final+" OR "+"("+type_list[i][0]+")"
	final1=[number,final]
	return final1



    def make_dc_f_table(self):
        q=self.create_pre_feature_query()
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        create_table='CREATE TABLE '+self.dataengine.dataset.spec_tb_name('dc_f_dd')+' AS SELECT * FROM ('+q+') as total_cells;'
        cursor.execute(create_table)
    
    def create_pre_feature_query(self):
        num_of_dc=len(self.denial_constraints)
        querie=self.make_queries()
        final_query=''
        for i in range(num_of_dc):
            for q in querie[i]:
                final_query+=q+' UNION '
        final_query=final_query[:-7]
        
        return final_query



    def create_init_value_spark(self,spark_session):
	dataframe=spark_session.get_table_spark("T")
	d=dataframe.columns
	inside=1
	for attri in  d:#atributes
	 if attri!="index":
		if inside==1:
			dataframe=dataframe.withColumn("attr_name", lit(attri))
			dataframe_sel=dataframe.select("index",attri,"attr_name").withColumnRenamed(attri, "atribute_value")
			inside=2
		else:
			dataframe=dataframe.withColumn("attr_name", lit(attri))
    	 		dataframe_sel=dataframe_sel.unionAll(dataframe.select("index",attri,"attr_name").withColumnRenamed(attri, "atribute_value"))
    	return dataframe_sel	

    def create_possible_value_spark(self, spark_session):
	dataframe=spark_session.get_table_spark("T")
	dataframe1=spark_session.get_table_spark("T")

	d=dataframe.columns
	inside=1
	for attri in  d:#atributes
	 if attri!="index":
		if inside==1:
			dataframe=dataframe.withColumn("attr_name", lit(attri))
			dataframe_cross=dataframe.crossJoin(dataframe1.select("index",attri).withColumnRenamed(attri, "val_name").withColumnRenamed("index", "index2")).select("index","attr_name","val_name").distinct().sort(col("index"))
			inside=2
		else:
			dataframe=dataframe.withColumn("attr_name", lit(attri))
			dataframe_cross1=dataframe.crossJoin(dataframe1.select("index",attri).withColumnRenamed(attri, "val_name").withColumnRenamed("index", "index2")).select("index","attr_name","val_name").distinct().sort(col("index"))
    	 		dataframe_cross=dataframe_cross.unionAll(dataframe_cross1)

    	return dataframe_cross
	
    def change_con(self,spark_session):
	dataframe=spark_session.get_table_spark("T")
	d=dataframe.columns
	dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        new_dcs=[]
	i=0
        for c in dc_sql_parts:
		list_preds,type_list=self.table_type(c)
		new_dcs.append(self.table_type_new(list_preds,d,i))
	return new_dcs

    
    def table_type_new(self,cond,d,number):
	operationsArr=['<>' , '<=' ,'>=','=' , '<' , '>']
	type_list=[]
	for i in range(0,len(cond)):
		list_preds=cond[i].split('.')
		string1=""
		for p in (0,len(list_preds)-1):
			if list_preds[p] in d:
				for text in operationsArr:
					if text in list_preds[p-1] :
						list3=list_preds[p-1].split(text)
						string1="table1.attr_name= '"+list_preds[p]+"' AND " + "table1.val_name"+text+list3[1]+"."+list_preds[p]
						break
				for k in range(0,len(cond)):
					if k!=i:
						string1=string1+" AND "+cond[k]
				type_list.append([string1])
				break
	final=""

	final=final+"("+type_list[0][0]+")"
	for i in (1,len(type_list)-1):
		final=final+" OR "+"("+type_list[i][0]+")"
	final1=[number,final]
	return final1
    
	

  
    def make_spark_table(self,spark_session,sqlContext):
	dataframe1=self.create_possible_value_spark(spark_session)
	dataframe=spark_session.get_table_spark("T")
	dataframe.registerTempTable("table2")
	dataframe1=dataframe.crossJoin(dataframe1.withColumnRenamed("index", "index2"))
	final1=self.change_con(spark_session)
	
	dataframe1.registerTempTable("table1")


	dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')

	for p in range (0, len(dc_sql_parts)):
		final=final1[p]

		final_number=final[0]
		final_condition=final[1]
		dc="'"+dc_sql_parts[0]+"'"
		query="""SELECT  distinct  table1.index as rv_index, table1.attr_name as assigned_val, table1.val_name as rv_attr, table2.index as tup_id, IF(table1.index2 IS NOT NULL, """+dc+""" ,"No dc") as DC_name  from table2, table1 where ("""+final_condition+""" )order BY rv_index,rv_attr,assigned_val,DC_name,tup_id"""
		final = sqlContext.sql(query)
		final.show()



	

	

    
    def make_queries(self):
        init_t_name=self.create_init_value()
        possib_t_name=self.create_possible_value()
        num_of_dcs,queries= self.create_new_relaxed_dc(init_t_name, possib_t_name)
        dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        
        relaxed_queries=[]
        for dc_cnt in range(num_of_dcs):
            dc_name=dc_sql_parts[dc_cnt]
            query=[]
            for relaxed in queries[dc_cnt]:                
                rv_index=relaxed[2]['table3'][0]
                if rv_index in relaxed[2]['table2']:
                    tup_id=relaxed[2]['table1'][0]
                if rv_index in relaxed[2]['table1']:
                    tup_id=relaxed[2]['table2'][0]
                tmp='SELECT '+rv_index+'.tid as rv_index,'+rv_index+'.attr_name as rv_attr,'+rv_index+'.attr_val as assigned_val,IF('+rv_index+'.tid  IS NOT NULL,"'+dc_name+'","BAD") as DC,'+tup_id+'.tid as tup_id FROM '+ relaxed[0] +' WHERE '+relaxed[1]    
                query.append(tmp)
        relaxed_queries.append(query)	

        return relaxed_queries
    
    
    def test(self):
        init_t_name=self.create_init_value()
        possib_t_name=self.create_possible_value()
        queries= self.create_new_dc(init_t_name, possib_t_name)
        cmd="SELECT "+queries[0][2]['table1'][0]+".tid as ind1,"+queries[0][2]['table2'][0]+".tid as ind2 FROM "+queries[0][0]+" WHERE "+queries[0][1]+";"
        return cmd    
        
    
    def create_init_value(self):
        
        db_name=self.dataengine.dbname
        table_name=self.dataengine.dataset.spec_tb_name('T')
        table_attribute_string=self.dataengine.get_schema('T')
        table_attribute=table_attribute_string.split(',')
        cursor = self.dataengine.data_engine.raw_connection().cursor()
        view_names=[]
        cell_init_view='CREATE VIEW '+db_name+'.value_init_'+table_name+' AS'

        index_name=table_name+'.index'
        for attr in table_attribute:
            if attr != 'index':
                cell_init_view+=' SELECT '+index_name+' as tid,IF('+index_name+' IS NOT NULL,"'+attr+'","Bad") as attr_name,'+attr+' as attr_val FROM '+table_name+' UNION '
        cell_init_view=cell_init_view[:-6]
        cell_init_view+=';'
        cursor.execute(cell_init_view)

        return db_name+'.value_init_'+table_name     
    
    
    
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
    
    
    def create_new_relaxed_dc(self,init_table_name,possib_table_name):    
        dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        new_dcs=self.create_relaxed_dc()
        result=[]
        num_of_dcs=len(dc_sql_parts)
        for dc_cnt in range(num_of_dcs):
            tmp=[]
            for v in new_dcs[dc_cnt]:
                tmp.append(self.make_new_cond(v[1],init_table_name,possib_table_name))
            result.append(tmp)
        return num_of_dcs,result
        
        
    
    def create_new_dc(self,init_table_name,possib_table_name):
        dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        new_dcs=[]
        for c in dc_sql_parts:
            new_dcs.append(self.make_new_cond(c,init_table_name,possib_table_name))
        return new_dcs
    
    def create_relaxed_dc(self):
        dcp=dcparser.DCParser(self.denial_constraints)
        dc_sql_parts=dcp.make_and_condition(conditionInd = 'all')
        relaxed_dcs=[]
        symmetric_op=['=','<>']
        for dc_sql in dc_sql_parts:
            tmp_list=[]
            preds=dc_sql.split('AND')
            num_of_preds=len(preds)
            
            for i in range(num_of_preds):
                op=self.find_op(preds[i])
                if 'table1' in preds[i]:
                    tmp=preds[i].replace('table1','table3')
                    tmp_list.append(['table2',dc_sql.replace(preds[i],tmp)])
                if 'table2' in preds[i] and op not in symmetric_op:
                    tmp=preds[i].replace('table2','table3')
                    tmp_list.append(['table1',dc_sql.replace(preds[i],tmp)])
            relaxed_dcs.append(tmp_list)
        return relaxed_dcs
    
    def make_new_cond(self,cond,init_table_name,possib_table_name):
        list_preds,type_list=self.table_type(cond)
        dict_l,new_list_preds=self.rel_network_preds(list_preds)
        
            
        new_cond=dict_l['table1'][0]+'.tid <>'+dict_l['table2'][0]+'.tid AND '+self.make_combinatorial_string(dict_l['table1'])+' AND '+self.make_combinatorial_string(dict_l['table2'])+' AND'
                
        for i in range(len(new_list_preds)):
            if type_list[i]==1:
                new_cond+=' '+self.create_new_pred_1arg(new_list_preds[i])+' AND '
            else :
                new_cond+=' '+self.create_new_pred_2arg(new_list_preds[i])+' AND '
        new_cond=new_cond[:-5]
        
        table_selection=''
        for t in list(set(dict_l['table1'])-set(dict_l['table3'])):
            table_selection+=init_table_name+' '+t+','
        for t in list(set(dict_l['table2'])-set(dict_l['table3'])):
            table_selection+=init_table_name+' '+t+','
        for t in dict_l['table3']:
            table_selection+=possib_table_name+' '+t+','       
                
            
        return table_selection[:-1],new_cond,dict_l
    
    def make_combinatorial_string(self,t_list):
        cmb=''
        for a1, a2 in itertools.combinations(t_list, 2):
            cmb+=a1+'.tid='+a2+'.tid AND '
        cmb=cmb[:-5]
        return cmb
    
    def rel_network_preds(self,list_preds):
        table_number=1
        a=[]
        b=[]
        c=[]
        dict_t_rel={'table1':a,'table2':b,'table3':c}
        for p in list_preds:
            if 'table1' in p:
                dict_t_rel['table1'].append('t'+str(table_number))
                list_preds[list_preds.index(p)]=p.replace('table1','t'+str(table_number))
                p=p.replace('table1','t'+str(table_number))
                table_number+=1
            if 'table2' in p:
                dict_t_rel['table2'].append('t'+str(table_number))
                list_preds[list_preds.index(p)]=p.replace('table2','t'+str(table_number))
                p=p.replace('table2','t'+str(table_number))
                table_number+=1
            if 'table3' in p and 'table1' not in p:
                dict_t_rel['table1'].append('t'+str(table_number))
                dict_t_rel['table3'].append('t'+str(table_number))
                list_preds[list_preds.index(p)]=p.replace('table3','t'+str(table_number))
                p=p.replace('table3','t'+str(table_number))               
                table_number+=1
            if 'table3' in p and 'table2' not in p:
                dict_t_rel['table2'].append('t'+str(table_number))
                dict_t_rel['table3'].append('t'+str(table_number))
                list_preds[list_preds.index(p)]=p.replace('table3','t'+str(table_number))
                p=p.replace('table3','t'+str(table_number))
                table_number+=1
        return dict_t_rel,list_preds
                    
    def table_type(self,cond):
        list_preds=cond.split(' AND ')
        type_list=[]
        for p in list_preds:
            type_list.append(self.find_pred_type(p))
        return list_preds,type_list      
        
    
    def create_new_pred_2arg(self,old_pred):
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
                new_pred=first_table+".attr_name='"+first_attr.strip()+"' AND "+second_table+".attr_name='"+second_attr.strip()+"' AND "+first_table+".attr_val"+oper+second_table+".attr_val"
                
                return new_pred
    
    def create_new_pred_1arg(self,old_pred):
        dcp=dcparser.DCParser(self.denial_constraints)
        op_set=dcp.operationsArr
        op_set=sorted(op_set, key=len, reverse=True)
        for oper in op_set:
            if oper in old_pred:
                pred_parts=old_pred.split(oper)
                first_table=pred_parts[0].split('.')[0]
                first_attr=pred_parts[0].split('.')[1]
                value=pred_parts[1]
                new_pred=first_table+".attr_name='"+first_attr.strip()+"' AND "+first_table+".attr_val"+oper+"'"+value+"'"
                
                return new_pred
    
    def find_op(self,pred):
        dcp=dcparser.DCParser(self.denial_constraints)
        op_set=dcp.operationsArr
        op_set=sorted(op_set, key=len, reverse=True)
        for oper in op_set:
            if oper in pred:
                return oper
    
    def find_pred_type(self,pred):
        typep=0
        if 'table1' in pred:
            typep+=1
        if 'table2' in pred:
            typep+=1
        if 'table3' in pred:
            typep+=1
        return typep
