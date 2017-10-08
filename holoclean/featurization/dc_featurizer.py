import sys
from distributed.diagnostics.progress_stream import counter
sys.path.append('../../')
from holoclean.utils import dcparser
import pyspark as ps
from numpy import unique
import sqlalchemy as sa
import itertools

class DCFeaturizer:
    
    def __init__(self,data_dataframe,denial_constraints,dataengine):
        self.data_dataframe=data_dataframe
        self.denial_constraints=denial_constraints
        self.dataengine=dataengine
    
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