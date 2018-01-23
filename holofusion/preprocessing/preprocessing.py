class Preprocessing:
    """TODO.
        create the feature table for numbskul
    """

    def __init__(self, spark_session, dataengine, dataset, path):
        """TODO.
                Parameters
                --------
                dataengine,dataset
                """
        self.spark_session = spark_session
        self.dataengine = dataengine
        self.dataset = dataset
        self.path_to_training_data = path
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

    def _flattening(self, name_of_table):
        table_rv_attr_string = self.dataengine.get_schema(
            self.dataset, name_of_table)
        attributes = table_rv_attr_string.split(',')

        while self.key not in attributes:
            self.key = raw_input("give the attribute that distinguish the objects:")

        counter = 0
        query_for_flattening = "CREATE TABLE \
                                " + self.dataset.table_specific_name(name_of_table+"_flat") \
                               + "(source TEXT, rv_index TEXT, \
                                rv_attr TEXT, assigned_val TEXT);"
        self.dataengine.query(query_for_flattening)

        for attribute in attributes:
            if attribute != self.key and attribute != "Source" and attribute != "source" \
                    and attribute != "Index" and attribute != 'index':
                query_for_featurization = """ (SELECT \
                                           table1.source, table1.""" + self.key + """ as rv_index,'""" + \
                                          attribute + """'  \
                                                              AS rv_attr, \
                                                              table1.""" + attribute + """ AS assigned_val \
                                                              FROM """ + \
                                          self.dataset.table_specific_name(name_of_table) + \
                                          " AS table1)"
                insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name(name_of_table+"_flat") + \
                                      " SELECT * FROM ( " + query_for_featurization + \
                                      "as T_" + str(counter) + ");"
                counter += 1
                self.dataengine.query(insert_signal_query)
        return


    # Setter

    def adding_training_data(self):
        self._key_attribute()
        self.ground_truth = self.spark_session.read.csv(self.path_to_training_data, header=True)
        self.dataengine.add_db_table('Training', self.ground_truth, self.dataset)
        return

    def creating_c_clean_table(self):
        if self.multiple_weights:
            self._flattening("Init")
            self._flattening("Training")

            mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('C_clean_flat') + \
                          " AS " \
                          "(SELECT table1.*" \
                          " FROM " + \
                          self.dataset.table_specific_name('Init_flat') + " AS table1, " + \
                          self.dataset.table_specific_name('Training_flat') + " AS table2 " \
                          "WHERE table1.rv_index=table2.rv_index and table1.rv_attr=table2.rv_attr" \
                          " and table1.assigned_val=table2.assigned_val);"

            self.dataengine.query(mysql_query)

        else:
            table_attribute_string = self.dataengine.get_schema(
                self.dataset, "Init")
            attributes = table_attribute_string.split(',')
            where_clause = ""
            for attribute in attributes:
                if attribute != self.key and attribute != "Source" and attribute != "source" \
                  and attribute != "Index" and attribute != 'index':
                    where_clause = where_clause + " and table1." + attribute + "=table2." + attribute

            mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('C_clean') + \
                          " AS " \
                          "(SELECT table1.*" \
                          " FROM " + \
                          self.dataset.table_specific_name('Init') + " AS table1, " + \
                          self.dataset.table_specific_name('Training') + " AS table2 " \
                          "WHERE table1."+self.key + "=table2." + self.key +  \
                          where_clause + ");"

            self.dataengine.query(mysql_query)

            schema = self.dataengine.get_schema(
                self.dataset, "Init")
            attributes = schema.split(',')

            self.dataengine._add_info_to_meta('C_clean', attributes, self.dataset)
            self._flattening("C_clean")
        return

    def creating_c_dk(self):

        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('C_dk') + \
                      " AS " \
                      "(SELECT table1.*" \
                      " FROM " + \
                      self.dataset.table_specific_name('Init') + " AS table1 LEFT JOIN " + \
                      self.dataset.table_specific_name('Training') + " AS table2 " \
                      "on table1."+self.key + "=table2."+self.key+" where table2."+self.key+" is NULL);"

        schema = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = schema.split(',')

        self.dataengine._add_info_to_meta('C_dk', attributes, self.dataset)
        self.dataengine.query(mysql_query)

        self._flattening('C_dk')
        return

    def creating_tables(self):
        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('C_dk') + \
                      " AS " \
                      "(SELECT table1.*" \
                      " FROM " + \
                      self.dataset.table_specific_name('Init') + " AS table1 );"
        self.dataengine.query(mysql_query)

        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('C_clean') + \
                      " LIKE " + \
                      self.dataset.table_specific_name('Init') + " ;"
        self.dataengine.query(mysql_query)

        schema = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = schema.split(',')

        self.dataengine._add_info_to_meta('C_dk', attributes, self.dataset)
        self._flattening('C_dk')


        self.dataengine._add_info_to_meta('C_clean', attributes, self.dataset)
        self._flattening('C_clean')


