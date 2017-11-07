import math


class variable:
    """TODO:variable class: class for creating variable"""
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Variable:

    """TODO:Variable class: Creates the domain table for all the cells"""
    def __init__(self,dataengine,dataset,spark_session):
        """TODO.
        Parameters
        --------
        spark_session:Takes as an argument the spark_Session 
        threshold:The threshold that we will use for the pruning
        dataengine:Takes as an argument the Data Engine to create Domain table
        """
    self.spark_session=spark_session
    self.dataengine=dataengine
    self.dataset=dataset

    self._c_values=self._c_values()
    def _c_values(self):
        """
        Create list of variables for numbskull
        """
    dataframe=self.dataengine._table_to_dataframe("Init",self.dataset)
    dataframe_Domain=self.dataengine._table_to_dataframe("Domain",self.dataset)
    dataframe_Domain.createOrReplaceTempView("Domain")
    
    dataframe_dk=self.dataengine._table_to_dataframe("C_dk",self.dataset)
    noisy_cells=[]
    for c in dataframe_dk.collect():
        noisy_cells.append([int(c[0]),c[1]])
        table_attribute=dataframe.columns
    rows=1
    cell_values=[]
    for c in dataframe.drop('index').collect():
        row={}
        j=1
        for i in c:
            attr_name="'"+table_attribute[j]+"'"
            Cardinality = self.spark_session.sql("SELECT * FROM Domain WHERE tid = "+ str(rows) +" AND attr_name = "+ attr_name )
            if [rows,table_attribute[j]] in noisy_cells:
                Evidence=0
            else:    
                Evidence=1    
            cell = variable(isEvidence=Evidence, Initial_Value=i,Datatype=1,Cardinality=Cardinality.count(),Vtf_offset=1)
            j=j+1
            cell_values.append(cell)
        rows=rows+1

    for i in cell_values:
        print [i.isEvidence, i.Initial_Value,i.Cardinality,i.Vtf_offset]

    return cell_values