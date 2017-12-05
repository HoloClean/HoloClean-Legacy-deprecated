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
        # query to create the probability table
        query_probability = "CREATE TABLE " + \
            self.dataset.table_specific_name('Probabilities') + " AS ("

        # The attributes of the probability table index,attribute,assigned
        # value, probability for each cell
        query_probability = query_probability + \
            " select table1.rv_attr,table1.rv_index,assigned_val,\
             sum_probabilities/total_sum as probability from"

        # First table of the query where get the sum of all the probabilities
        # for each distinct group of rv_attr,rv_index
        query_probability = query_probability + " (select sum(sum_probabilities) as total_sum ,\
            rv_index,rv_attr from (select exp(sum(0+weight_val)) \
            as sum_probabilities ,rv_attr ,\
            rv_index,assigned_val from " + self.dataset.table_specific_name(
            'Weights') + " as table1, " + self.dataset.table_specific_name('Feature') + " as table2 where table1.weight_id=table2.weight_id \
            group by rv_attr,rv_index,assigned_val) as \
             table1 group by rv_attr,rv_index)as table2 ,"

        # Second table of the query where get the probabilities for each
        # distinct group of rv_attr,rv_index,assigned_val
        query_probability = query_probability + "(select exp(sum(0+weight_val)) as sum_probabilities ,rv_attr ,\
            rv_index,assigned_val from " + self.dataset.table_specific_name(
            'Weights') + " as table1, " + self.dataset.table_specific_name(
            'Feature') + " as table2 where table1.weight_id=table2.weight_id group by\
            rv_attr,rv_index,assigned_val) as table1"

        query_probability = query_probability + \
            " where table1.rv_index=table2.rv_index \
            and table1.rv_attr=table2.rv_attr) ; "
        self.dataengine.query(query_probability)

        # Query to find the repair for each cell
        query = "CREATE TABLE " + self.dataset.table_specific_name(
            'Final') + " AS (" + " select table1.rv_index, \
            table1.rv_attr, max(assigned_val) as assigned_val from \
            (select max(probability) as max1 ,rv_index , \
            rv_attr   from " + self.dataset.table_specific_name(
            'Probabilities') + " group by rv_attr,rv_index)\
            as table1 , " + self.dataset.table_specific_name(
            'Probabilities') + " as table2, \
            " + self.dataset.table_specific_name(
            'C_dk') + " as table3 where table1.rv_index=table2.rv_index \
            and table1.rv_attr=table2.rv_attr \
            and max1=table2.probability and table3.ind=table1.rv_index \
            and table3.attr=table1.rv_attr \
            group by table1.rv_index,table1.rv_attr);"
        self.dataengine.query(query)

        # We get the number of repairs form the dont know cells
        dataframe1 = self.dataengine._table_to_dataframe("C_dk", self.dataset)
        number_of_repairs = dataframe1.count()

        table_attribute_string = self.dataengine._get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        create_changes_table = "CREATE TABLE \
        " + self.dataset.table_specific_name(
            'Changes') + " AS (SELECT * FROM \
        " + self.dataset.table_specific_name('Init') + ");"
        self.dataengine.query(create_changes_table)
        query = ""
        for attribute in attributes:
            if attribute != "index":
                query = query + "update " + self.dataset.table_specific_name('Changes') + "\
                    as table1 , " + self.dataset.table_specific_name(
                    'Final') + " as table2  SET table1." + attribute + "=table2.assigned_val \
                    where table1.index=table2.rv_index \
                    and rv_attr='" + attribute + "';"

        self.dataengine.query(query
                              )
