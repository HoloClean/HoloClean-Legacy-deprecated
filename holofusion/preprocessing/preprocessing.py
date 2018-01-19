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
        self.attribute_to_check = ""

    def key_attribute(self):
        table_attribute_string = self.dataengine.get_schema(
            self.dataset, "Init")
        attributes = table_attribute_string.split(',')
        print attributes
        # self.key = raw_input("give the attribute that distinguish the objects:")
        while self.key not in attributes:
            self.key = raw_input("give the attribute that distinguish the objects:")
        return

    def adding_training_data(self):
        self.ground_truth = self.spark_session.read.csv(self.path_to_training_data, header=True)
        self.dataengine.add_db_table('Training', self.ground_truth, self.dataset)
        return

    def creating_c_clean_table(self):
        self.attribute_to_check = "last"

        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('C_clean') + \
                      " AS " \
                      "(SELECT table1.*" \
                      " FROM " + \
                      self.dataset.table_specific_name('Init') + " AS table1, " + \
                      self.dataset.table_specific_name('Training') + " AS table2 " \
                      "WHERE table1."+self.key + "=table2." + self.key + \
                      " and table1."+self.attribute_to_check + "=table2."+self.attribute_to_check + ");"
        self.dataengine.query(mysql_query)
        return

    def creating_c_dk(self):
        mysql_query = "CREATE TABLE " + self.dataset.table_specific_name('C_dk') + \
                      " AS " \
                      "(SELECT table1.*" \
                      " FROM " + \
                      self.dataset.table_specific_name('Init') + " AS table1 LEFT JOIN " + \
                      self.dataset.table_specific_name('Training') + " AS table2 " \
                      "on table1."+self.key + "=table2."+self.key+" where table2."+self.key+" is NULL);"
        print mysql_query
        self.dataengine.query(mysql_query)

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
