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
            " AS (" \
            "SELECT EXP(SUM(0+weight_val)) AS sum_probabilities," \
            "rv_attr," \
            "rv_index," \
            "assigned_val " \
            "FROM " + \
                        self.dataset.table_specific_name('Weights') + " AS table1," +\
                        self.dataset.table_specific_name('Feature') + " AS table2 " \
                                                                      "WHERE " \
                                                                      "table1.weight_id=table2.weight_id " \
                                                                      "GROUP BY " \
                                                                      "rv_attr,rv_index,assigned_val);"

        self.dataengine.query(query_Feature)
        
        query_probability = "CREATE TABLE " + self.dataset.table_specific_name('Probabilities') + \
                            " AS " \
                            "(SELECT " \
                            "table1.rv_attr," \
                            "table1.rv_index," \
                            "table1.assigned_val," \
                            "sum_probabilities/total_sum AS probability " \
                            "FROM " + \
                            self.dataset.table_specific_name('Feature_gb_accur') + " AS table1," \
                                                                                   "(SELECT " \
                                                                                   "SUM(sum_probabilities) " \
                                                                                   "AS total_sum," \
                                                                                   "rv_index," \
                                                                                   "rv_attr " \
                                                                                   "FROM " + \
                            self.dataset.table_specific_name('Feature_gb_accur') + \
                            " GROUP BY " \
                            "rv_attr,rv_index) " \
                            "AS table2 " \
                            "WHERE " \
                            "table1.rv_attr=table2.rv_attr " \
                            "AND " \
                            "table1.rv_index=table2.rv_index);"

        self.dataengine.query(query_probability)

        # Query to find the repair for each cell
        query = "CREATE TABLE " + self.dataset.table_specific_name('Final') + \
                " AS (" \
                "SELECT " \
                "table1.rv_index," \
                "table1.rv_attr," \
                "MAX(table2.assigned_val) AS assigned_val " \
                "FROM (" \
                "SELECT " \
                "MAX(probability) AS max1," \
                "rv_index," \
                "rv_attr " \
                "FROM " + \
                self.dataset.table_specific_name('Probabilities') + \
                " GROUP BY rv_attr,rv_index) AS table1 , " +\
                self.dataset.table_specific_name('Probabilities') + " AS table2," + \
                self.dataset.table_specific_name('C_dk') + " AS table3 " \
                                                           "WHERE " \
                                                           "table1.rv_index = table2.rv_index " \
                                                           "AND " \
                                                           "table1.rv_attr = table2.rv_attr " \
                                                           "AND " \
                                                           "max1 = table2.probability " \
                                                           "AND " \
                                                           "table3.ind = table1.rv_index " \
                                                           "AND " \
                                                           "table3.attr = table1.rv_attr " \
                                                           "GROUP BY " \
                                                           "table1.rv_attr,table1.rv_index" \
                                                           ");"
            
        self.dataengine.query(query)

    def printing_results(self, threshold, k_first):
        if k_first != 0:
            query = "set @type = '';set @num  = 1;set @type2 = '';"
            self.dataengine.query(query)
            query = "CREATE TABLE " + self.dataset.table_specific_name('k_Probabilities') + \
                    " AS (" \
                    "SELECT " \
                    "rv_attr,rv_index ,assigned_val, probability, " \
                    "@num := if(@type = rv_attr and @type2 = rv_index, @num + 1, 1) as row_number," \
                    "@type := rv_attr as dummy, @type2 :=rv_index as dummy2 from " + \
                    self.dataset.table_specific_name('Probabilities') + \
                    " ORDER BY rv_index, rv_attr, probability desc);"
            self.dataengine.query(query)
            final = self.dataengine.get_table_to_dataframe("k_Probabilities", self.dataset)
            final.createOrReplaceTempView("probabilities")
            sql = self.spark_session.sql("SELECT * FROM probabilities where probability >= "+str(threshold) +
                                         " and row_number<="+str(k_first))

        else:
            final = self.dataengine.get_table_to_dataframe("Probabilities", self.dataset)
            final.createOrReplaceTempView("probabilities")
            sql = self.spark_session.sql("SELECT * FROM probabilities where probability>="+str(threshold))
        sql.select("row_number", "rv_index", "rv_attr", "assigned_val", "probability").show()
