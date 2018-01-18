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

    def key_attribute(self):
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        print attributes
        # self.key = raw_input("give the attribute that distinguish the objects:")
        while self.key not in attributes:
            self.key = raw_input("give the attribute that distinguish the objects:")
        return

    def add_weights(self):
        """
        This method updates the values of weights for the featurization table"
        """
        print('creating weight table')
        query_for_weights = "CREATE TABLE " \
                            + self.dataset.table_specific_name('weight_temp') \
                            + "(" \
                              "weight_id INT PRIMARY KEY AUTO_INCREMENT," \
                              "Source_id LONGTEXT" \
                              ");"

        self.dataengine.query(query_for_weights)

        query = "INSERT INTO  " \
                + self.dataset.table_specific_name('weight_temp') + \
                " (" \
                "SELECT * FROM (" \
                "SELECT distinct NULL, Source_id FROM " + \
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
                              " table1.Source_id=table2.Source_id ) " \
                              "AS ftmp  order by rv_index,rv_attr" \
                              " );"

        self.dataengine.query(query_featurization)
        return

    def create_feature(self):
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

        for attribute in attributes:
            if attribute != self.key and attribute != "Source" and attribute != "Index" and attribute != 'index':
                # INSERT statement for training data
                query_for_featurization_clean = """ (SELECT  @p := @p + 1 AS var_index,\
                                          Source AS Source_id,\
                                          init."""+self.key + """ as key_id,'""" + attribute+"""'  \
                                          AS attribute, \
                                          init."""+attribute+""" AS source_observation ,\
                                          ' 'AS weight_id, 1 AS fixed\
                                          FROM """ +\
                                          self.dataset.table_specific_name('C_clean') +\
                                          " AS init where init."+attribute+" IS NOT NULL)"
                insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name('Feature_temp') + \
                                      " SELECT * FROM ( " + query_for_featurization_clean + \
                                      "as T_" + str(counter) + ");"
                counter += 1
                print insert_signal_query
                self.dataengine.query(insert_signal_query)
                global_counter = "select max(var_index) into @p from " + \
                                 self.dataset.table_specific_name('Feature_temp') + ";"
                self.dataengine.query(global_counter)

                # INSERT statement for non training data
                query_for_featurization_dk = """ (SELECT  @p := @p + 1 AS var_index,\
                                                          Source AS Source_id,\
                                                          init.""" + self.key + """ as key_id,'""" + attribute + """'  \
                                                          AS attribute, \
                                                          init.""" + attribute + """ AS source_observation ,\
                                                          ' 'AS weight_id, 0 AS fixed\
                                                          FROM """ + \
                                                self.dataset.table_specific_name('C_dk') + \
                                                " AS init where init." + attribute + " IS NOT NULL)"
                insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name('Feature_temp') + \
                                      " SELECT * FROM ( " + query_for_featurization_dk + \
                                      "as T_" + str(counter) + ");"
                counter += 1
                print insert_signal_query
                self.dataengine.query(insert_signal_query)
                global_counter = "select max(var_index) into @p from " + \
                                 self.dataset.table_specific_name('Feature_temp') + ";"
                self.dataengine.query(global_counter)

        return
