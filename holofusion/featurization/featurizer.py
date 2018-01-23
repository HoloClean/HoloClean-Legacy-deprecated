class Featurizer:
    """TODO.
        create the feature table for numbskul
    """

    def __init__(self, dataengine, dataset):
        """TODO.
                Parameters
                --------
                dataengine,dataset
         """
        self.dataengine = dataengine
        self.dataset = dataset
        self.key = dataengine.holoEnv.key
        self.attribute_to_check = dataengine.holoEnv.attribute_to_check
        self.multiple_weights = dataengine.holoEnv.multiple_weights

    def _key_attribute(self):
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        print attributes
        # self.key = raw_input("give the attribute that distinguish the objects:")
        while self.key not in attributes:
            self.key = raw_input("give the attribute that distinguish the objects:")
        return

    # Setters
    def add_weights(self):
        """
        This method updates the values of weights for the featurization table"
        """

        one_attribute = 1
        if self.multiple_weights:
            query_weight = " Source_id LONGTEXT, attribute LONGTEXT "
            query_select = " SELECT distinct NULL, Source_id, attribute FROM "
            query_where = " table1.Source_id=table2.Source_id and table1.attribute=table2.attribute )"
        else:
            query_weight = " Source_id LONGTEXT "
            query_select = " SELECT distinct NULL, Source_id  FROM "
            query_where = " table1.Source_id=table2.Source_id  )"

        print('creating weight table')
        query_for_weights = "CREATE TABLE " \
                            + self.dataset.table_specific_name('weight_temp') \
                            + "(" \
                              "weight_id INT PRIMARY KEY AUTO_INCREMENT," \
                            + query_weight + \
                              ");"

        self.dataengine.query(query_for_weights)

        query = "INSERT INTO  " \
                + self.dataset.table_specific_name('weight_temp') + \
                " (" \
                "SELECT * FROM (" \
                + query_select +\
                self.dataset.table_specific_name('Feature_temp') + "" \
                ") AS TABLE1);"

        self.dataengine.query(query)
        print('creating feature table with weights ids')
        create_feature_table_query = "CREATE TABLE \
                                     " + self.dataset.table_specific_name('Feature') \
                                     + "(var_index INT PRIMARY KEY AUTO_INCREMENT,Source_id TEXT , rv_index TEXT,\
	                                   rv_attr TEXT,assigned_val   TEXT," \
                                       " weight_id TEXT, fixed INT );"

        self.dataengine.query(create_feature_table_query)

        query_featurization = "INSERT INTO " + self.dataset.table_specific_name('Feature') + \
                              " (" \
                              "SELECT * FROM ( SELECT " \
                              "NULL AS var_index" \
                              " , table1.Source_id" \
                              " , table1.key_id as rv_index" \
                              ", table1.attribute as rv_attr" \
                              " , table1.source_observation as assigned_val" \
                              " ,  table2.weight_id, table1.fixed as fixed" \
                              " FROM " \
                              + self.dataset.table_specific_name('Feature_temp') + " AS table1, " \
                              + self.dataset.table_specific_name('weight_temp') + " AS table2 " \
                              " WHERE" \
                              + query_where +\
                              "AS ftmp  order by rv_index,rv_attr" \
                              " );"

        self.dataengine.query(query_featurization)
        return

    def create_feature(self):
        self._key_attribute()
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        counter = 0
        global_counter = "set @p:=0;"
        self.dataengine.query(global_counter)

        query_for_featurization = "CREATE TABLE \
            " + self.dataset.table_specific_name('Feature_temp') \
            + "(var_index INT,Source_id TEXT, key_id TEXT, \
            attribute TEXT, source_observation TEXT," \
            " weight_id TEXT, fixed INT);"
        self.dataengine.query(query_for_featurization)
        query_for_featurization_clean = """ (SELECT  @p := @p + 1 AS var_index,\
                                                            table1.source AS Source_id,\
                                                            table1.rv_index  as key_id,table1.rv_attr  \
                                                            AS attribute, \
                                                            table1.assigned_val AS source_observation ,\
                                                            ' 'AS weight_id, 1 AS fixed\
                                                            FROM """ + \
                                        self.dataset.table_specific_name('C_clean_flat') + \
                                        " AS table1 where table1.assigned_val IS NOT NULL)"
        
        insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name('Feature_temp') + \
                              " SELECT * FROM ( " + query_for_featurization_clean + \
                              "as T_" + str(counter) + ");"
        counter += 1
        self.dataengine.query(insert_signal_query)
        global_counter = "select max(var_index) into @p from " + \
                         self.dataset.table_specific_name('Feature_temp') + ";"
        self.dataengine.query(global_counter)

        query_for_featurization_clean = """ (SELECT  @p := @p + 1 AS var_index,\
                                                            table1.source AS Source_id,\
                                                            table1.rv_index  as key_id,table1.rv_attr  \
                                                            AS attribute, \
                                                            table1.assigned_val AS source_observation ,\
                                                            ' 'AS weight_id, 0 AS fixed\
                                                            FROM """ + \
                                        self.dataset.table_specific_name('C_dk_flat') + \
                                        " AS table1 where table1.assigned_val IS NOT NULL)"
        
        insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name('Feature_temp') + \
                              " SELECT * FROM ( " + query_for_featurization_clean + \
                              "as T_" + str(counter) + ");"

        counter += 1
        self.dataengine.query(insert_signal_query)
        global_counter = "select max(var_index) into @p from " + \
                         self.dataset.table_specific_name('Feature_temp') + ";"
        self.dataengine.query(global_counter)
        return

    def create_source_table(self):
        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('Sources') + \
            " AS " + "(SELECT DISTINCT " + self.key + ", count(source) FROM " \
                      + self.dataset.table_specific_name('C_dk') \
                      + " GROUP BY " + self.key + ")"
        self.dataengine.query(mysql_query)

    def create_fact_table(self):
        counter = 0
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        query_for_fact_table = "CREATE TABLE \
            " + self.dataset.table_specific_name('Fact') \
            + "(RID INT,key_id TEXT, \
            attribute TEXT, source_observation TEXT);"
        self.dataengine.query(query_for_fact_table)
        global_counter = "set @p:=0;"
        self.dataengine.query(global_counter)
        for attribute in attributes:
            if attribute != self.key and attribute != "Source" and attribute != "source" \
                    and attribute != "Index" and attribute != 'index':
                if attribute == self.attribute_to_check:
                    # INSERT statement for training data
                    query_for_fact = """ (SELECT  DISTINCT  @p := @p + 1 AS RID,\
        		                        init.""" + self.key + """ as key_id,'""" + attribute + """' AS attribute, \
        		                        init.""" + attribute + """ AS source_observation \
                                        FROM """ + \
                                        self.dataset.table_specific_name('C_dk') + \
                                        " AS init) "
                    insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name('Fact') + \
                                          " SELECT * FROM ( " + query_for_fact + " as T_" + str(counter) + ");"
                    counter += 1
                    print insert_signal_query
                    self.dataengine.query(insert_signal_query)
                    global_counter = "select max(RID) into @p from " + \
                                     self.dataset.table_specific_name('Fact') + ";"
                    self.dataengine.query(global_counter)
