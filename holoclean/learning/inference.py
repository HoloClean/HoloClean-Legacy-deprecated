import numpy as np


class inference:
    """TODO:
     This class create the probability table
     and finds the accuracy of our program
    """

    def __init__(self, dataengine, dataset, spark_session):
        """
        This constructor gets as arguments the
        dataengine,dataset and spark_session from the Session class
        """
        self.dataengine = dataengine
        self.dataset = dataset
        self.spark_session = spark_session

    def learning(self):
        """
        To do: creates the probability table for our
        dataset and checks the accuracy
        """

        query_Feature = "CREATE TABLE " + self.dataset.table_specific_name('Feature_gb_accur') +\
            " AS ( SELECT exp(sum(0+weight_val)) AS sum_probabilities  , rv_attr , rv_index , assigned_val from " + self.dataset.table_specific_name(
            'Weights') + " as table1, " + self.dataset.table_specific_name('Feature') + " as table2 where table1.weight_id=table2.weight_id \
            group by rv_attr,rv_index,assigned_val) ;"

        self.dataengine.query(query_Feature ) 
           
       # print query_Feature
       # raw_input("prompt")
        
        query_probability= "CREATE TABLE " + \
            self.dataset.table_specific_name('Probabilities') + \
            " AS (select table1.rv_attr, table1.rv_index, table1.assigned_val , \
            sum_probabilities/total_sum as probability from "+ self.dataset.table_specific_name('Feature_gb_accur') + " as table1," +\
            "(select sum(sum_probabilities) as total_sum ,rv_index, rv_attr from  " + self.dataset.table_specific_name('Feature_gb_accur') +\
            " group by rv_attr, rv_index)" +\
            " as table2 where table1.rv_attr=table2.rv_attr and  table1.rv_index=table2.rv_index);"

        raw_input("asd")

        self.dataengine.query(query_probability)

        # Query to find the repair for each cell
        query = "CREATE TABLE " + self.dataset.table_specific_name(
            'Final') + " AS (" + " select table1.rv_index, \
            table1.rv_attr, max(table2.assigned_val) as assigned_val from \
            (select max(probability) as max1 ,rv_index , \
            rv_attr   from " + self.dataset.table_specific_name(
            'Probabilities') + " group by rv_attr,rv_index)\
            as table1 , " + self.dataset.table_specific_name(
            'Probabilities') + " as table2, \
            " + self.dataset.table_specific_name(
            'C_dk') + " as table3 where table1.rv_index=table2.rv_index \
            and table1.rv_attr=table2.rv_attr \
            and max1=table2.probability and table3.ind=table1.rv_index \
            and table3.attr=table1.rv_attr group by table1.rv_attr,table1.rv_index);"
            
        self.dataengine.query(query)


