from pyspark.sql.types import *
from ..global_variables import GlobalVariables


class RandomVar:
    """RandomVar class: class for random variable"""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Pruning:
    """Pruning class: Creates the domain table for all the cells"""

    def __init__(self, session, threshold=0.5):
        """

            :param session: Holoclean session
            :param threshold: the threshold will use for pruning
        """
        self.session = session
        self.spark_session = session.holo_env.spark_session
        self.dataengine = session.holo_env.dataengine
        self.threshold = threshold
        self.dataset = session.dataset
        self.assignments = {}
        self.cell_domain_nb = {}
        self.domain_stats = {}
        self.domain_pair_stats = {}
        self.column_to_col_index_dict = {}
        self.attribute_to_be_pruned = {}
        self.dirty_cells_attributes = set([])
        self.cell_nbs = {}
        self.cooocurance_for_first_attribute = {}
        self.cell_domain = {}
        self.all_cells = []
        self.all_cells_temp = {}
        self.index = 0

        self.cellvalues = self._c_values()
        self.noisycells = self._d_cell()

        self._preprop()
        self._analyze_entries()
        self._generate_assignments()
        self._generate_coocurences()
        self._find_cell_domain()
        self._create_dataframe()

    # Internal Method
    def _d_cell(self):
        """
                Create noisy_cell list from the C_dk table
        """
        dataframe_dont_know = \
            self.dataengine.get_table_to_dataframe("C_dk", self.dataset)
        noisy_cells = []
        self.noisy_list = []
        for cell in dataframe_dont_know.collect():
            cell_variable = RandomVar(columnname=cell[1], row_id=int(cell[0]))
            noisy_cells.append(cell_variable)
            self.noisy_list.append([cell[1], int(cell[0])])
            self.cellvalues[int(cell[0])-1][
                 self.attribute_map[cell[1]]].dirty = 1

        return noisy_cells

    def _c_cell(self):
        """
                Create clean_cell list from the C_clean table
        """
        dataframe_clean = \
            self.dataengine.get_table_to_dataframe("C_clean", self.dataset)
        clean_cells = []
        self.clean_list = []
        for cell in dataframe_clean.collect():
            cell_variable = RandomVar(columnname=cell[1], row_id=int(cell[0]))
            clean_cells.append(cell_variable)
            self.clean_list.append([cell[1], int(cell[0])])

        return clean_cells

    def _c_values(self):
        """
                Create c_value list from the init table
        """
        dataframe_init = \
            self.dataengine.get_table_to_dataframe("Init", self.dataset)
        table_attribute = dataframe_init.columns
        row_id = 0
        cell_values = {}
        self.attribute_map = {}
        number_id = 0
        for column in dataframe_init.drop(GlobalVariables.index_name).collect():
            row = {}
            column_id = 0
            for column_value in column:
                self.attribute_map[table_attribute[column_id]] = column_id
                cell_variable = RandomVar(columnname=table_attribute[column_id],
                                          value=column_value, tupleid=row_id,
                                          cellid=number_id, dirty=0, domain=0)
                row[column_id] = cell_variable
                number_id = number_id + 1
                column_id = column_id + 1
            cell_values[row_id] = row
            row_id = row_id + 1
        return cell_values

    def _compute_number_of_coocurences(
            self,
            original_attribute,
            original_attr_value,
            cooccured_attribute,
            cooccured_attr_value):
        """
        generate_assignments creates assignment for each cell with the attribute
        and value of each other cell in the same row

        :param original_attribute: the name of first attribute
        :param original_attr_value: the initial value of the first attribute
        :param cooccured_attribute: the name of second attribute
        :param cooccured_attr_value: the initial value of the second attribute
        :return:
        """
        if (original_attr_value, cooccured_attr_value) not in \
                self.domain_pair_stats[original_attribute][cooccured_attribute]:
            return None
        cooccur_count = \
            self.domain_pair_stats[original_attribute][cooccured_attribute][(
                 original_attr_value, cooccured_attr_value)]
        v_cnt = self.domain_stats[original_attribute][original_attr_value]

        # Compute counter
        number_of_cooccurence = cooccur_count
        total_number_of_original_attr_value = v_cnt
        return number_of_cooccurence / total_number_of_original_attr_value

    def _find_domain(self, assignment, trgt_attr):
        """ This method finds the domain for each cell

           :param assignment: the values for every attribute
           :param trgt_attr: the name of attribute
        """
        cell_values = {(assignment[trgt_attr])}
        for attr in assignment:
            if attr == trgt_attr:
                continue
            attr_val = assignment[attr]
            if attr in self.cooocurance_for_first_attribute:
                if attr_val in self.cooocurance_for_first_attribute[attr]:
                    if trgt_attr in self.cooocurance_for_first_attribute[
                                    attr][attr_val]:
                        cell_values |= set(
                            self.cooocurance_for_first_attribute[attr][
                                attr_val][trgt_attr].keys())

        return cell_values

    def _preprop(self):
        """
               preprocessing phase: create the dictionary with
               all the attributes.
        """

        # This part creates dictionary to find column index

        row_dictionary_element = self.cellvalues[1]
        for cid in row_dictionary_element:
            cell = row_dictionary_element[cid]
            self.column_to_col_index_dict[cell.columnname] = cid

        # This part gets the attributes of noisy cells
        for cell in self.noisycells:
            self.dirty_cells_attributes.add(cell.columnname)

        # This part makes empty dictionary for each atribute
        for col in self.column_to_col_index_dict:
            self.domain_stats[col] = {}

        # This part adds other (dirty) attributes
        # to the dictionary of the key attribute
        for column_name_key in self.column_to_col_index_dict:
            self.domain_pair_stats[column_name_key] = {}
            for col2 in self.dirty_cells_attributes:
                if col2 != column_name_key:
                    self.domain_pair_stats[column_name_key][col2] = {}
        return

    def _analyze_entries(self):
        """
                analyzeEntries creates a dictionary with
                occurrences of the attributes
        """
        # Iterate over tuples to create to dictionary
        for tupleid in self.cellvalues:
            # Iterate over attributes and grab counts for create dictionary that
            #  show for each attribute how many times we see each value
            for cid in self.cellvalues[tupleid]:
                cell = self.cellvalues[tupleid][cid]
                col = cell.columnname
                val = cell.value
                if col in self.dirty_cells_attributes:
                    self.all_cells.append(cell)
                    self.cellvalues[tupleid][cid].domain = 1
                self.all_cells_temp[cell.cellid] = cell

                if val not in self.domain_stats[col]:
                    self.domain_stats[col][val] = 0.0
                self.domain_stats[col][val] += 1.0

            # Iterate over target attributes and grab
            # counts of values with other attributes
            for col in self.domain_pair_stats:
                cid = self.column_to_col_index_dict[col]
                for tgt_col in self.domain_pair_stats[col]:
                    tgt_cid = self.column_to_col_index_dict[tgt_col]
                    tgt_val = self.cellvalues[tupleid][tgt_cid].value
                    val = self.cellvalues[tupleid][cid].value
                    assgn_tuple = (val, tgt_val)
                    if assgn_tuple not in self.domain_pair_stats[col][tgt_col]:
                        self.domain_pair_stats[col][tgt_col][assgn_tuple] = 0.0
                    self.domain_pair_stats[col][tgt_col][assgn_tuple] += 1.0
        return

    def _generate_coocurences(self):
        """TO DO:
                _generate_coocurences creates candidates repairs
        """
        for original_attribute in self.domain_pair_stats:
            # For each column in the cooccurences
            self.cooocurance_for_first_attribute[original_attribute] = {}
            # It creates a dictionary
            for cooccured_attribute in \
                    self.domain_pair_stats[original_attribute]:
                # For second column in the cooccurences Over
                # Pair of values that happend with each other
                # (original_attribute value , cooccured_attribute value)
                for assgn_tuple in self.domain_pair_stats[
                                        original_attribute][
                                        cooccured_attribute]:
                    cooccure_number = self._compute_number_of_coocurences(
                        original_attribute, assgn_tuple[0], cooccured_attribute,
                        assgn_tuple[1])
                    if cooccure_number > self.threshold:
                        if assgn_tuple[0] not in\
                                self.cooocurance_for_first_attribute[
                                     original_attribute]:
                            self.cooocurance_for_first_attribute[
                                original_attribute][assgn_tuple[0]] = {}
                        if cooccured_attribute not in \
                                self.cooocurance_for_first_attribute[
                                    original_attribute][assgn_tuple[0]]:
                            self.cooocurance_for_first_attribute[
                                original_attribute][
                                assgn_tuple[0]][cooccured_attribute] = {}
                        self.cooocurance_for_first_attribute[
                            original_attribute][assgn_tuple[0]][
                            cooccured_attribute][
                            assgn_tuple[1]] = cooccure_number
        return

    def _generate_assignments(self):
        """
        generate_assignments creates assignment for each cell with the attribute
        and value of each other cell in the same row

        :return:
        """
        for cell in self.all_cells:
            tplid = cell.tupleid
            trgt_attr = cell.columnname

            # assignment is a dictionary for each cell copied
            # the row of that cell
            # with all attribute to find the cooccurance
            assignment = {}
            for cid in self.cellvalues[tplid]:
                c = self.cellvalues[tplid][cid]
                assignment[c.columnname] = c.value
            self.assignments[cell.cellid] = assignment
            self.attribute_to_be_pruned[cell.cellid] = trgt_attr
        return

    def _find_cell_domain(self):
        """
        find_cell_domain finds the domain for each cell
        :return:
        """
        for cell_index in self.assignments:
            # In this part we get all values for cell_index's
            # attribute_to_be_pruned
            self.cell_domain[cell_index] = self._find_domain(
                self.assignments[cell_index],
                self.attribute_to_be_pruned[cell_index])
        return

    def _append_possible(self, v_id, value, dataframe, cell_index, k_ij):
        if value != self.all_cells_temp[cell_index].value:
            dataframe.append(
                [v_id, (self.all_cells_temp[cell_index].tupleid + 1),
                 self.all_cells_temp[cell_index].columnname,
                 unicode(value), 0, k_ij])
        else:
            dataframe.append(
                [v_id, (self.all_cells_temp[cell_index].tupleid + 1),
                 self.all_cells_temp[cell_index].columnname,
                 unicode(value), 1, k_ij])

    def _create_dataframe(self):
        """
        creates a spark dataframe from cell_domain for all the cells
        :return:
        """
        attributes = self.dataengine.get_schema(self.dataset, 'Init').split(',')
        self.domain_dict = {}
        domain_kij_clean = []
        domain_kij_dk = []
        for attribute in attributes:
            if attribute != GlobalVariables.index_name:
                self.domain_dict[attribute] = []

        possible_values_clean = []
        possible_values_dirty = []
        c_clean = []
        c_dk = []
        v_id_clean = v_id_dk = 0
        for tuple_id in self.cellvalues:
            for cell_index in self.cellvalues[tuple_id]:
                attribute = self.cellvalues[tuple_id][cell_index].columnname
                value = self.cellvalues[tuple_id][cell_index].value
                if value not in self.domain_dict[attribute]:
                    self.domain_dict[attribute].append(value)

                if self.cellvalues[tuple_id][cell_index].dirty == 1:
                    c_dk.append([tuple_id + 1, attribute, value])
                    tmp_cell_index = \
                        self.cellvalues[tuple_id][cell_index].cellid
                    if self.cellvalues[tuple_id][cell_index].domain == 1:
                        k_ij = 0
                        v_id_dk = v_id_dk + 1
                        for value in self.cell_domain[tmp_cell_index]:
                            k_ij = k_ij + 1
                            self._append_possible(v_id_dk, value,
                                                  possible_values_dirty,
                                                  tmp_cell_index, k_ij)
                        domain_kij_dk.append([v_id_dk, (
                                self.all_cells_temp[tmp_cell_index].tupleid
                                + 1),
                                self.all_cells_temp[tmp_cell_index].columnname,
                                k_ij])
                else:
                    c_clean.append([tuple_id + 1, attribute, value])
                    tmp_cell_index = \
                        self.cellvalues[tuple_id][cell_index].cellid
                    if self.cellvalues[tuple_id][cell_index].domain == 1:
                        k_ij = 0
                        v_id_clean = v_id_clean + 1
                        for value in self.cell_domain[tmp_cell_index]:
                            k_ij = k_ij + 1
                            self._append_possible(v_id_clean, value,
                                                  possible_values_clean,
                                                  tmp_cell_index, k_ij)
                        domain_kij_clean.append([v_id_clean,
                                                 (self.all_cells_temp[
                                                      tmp_cell_index].tupleid
                                                  + 1),
                                                 self.all_cells_temp[
                                                     tmp_cell_index].columnname,
                                                 k_ij])

        # Create possible table
        new_df_possible = self.spark_session.createDataFrame(
            possible_values_clean, StructType([
                StructField("vid", IntegerType(), True),
                StructField("tid", IntegerType(), False),
                StructField("attr_name", StringType(), False),
                StructField("attr_val", StringType(), False),
                StructField("observed", IntegerType(), False),
                StructField("domain_id", IntegerType(), False),
            ])
        )
        self.dataengine.add_db_table('Possible_values_clean',
                                     new_df_possible, self.dataset)
        self.dataengine.add_db_table_index(
            self.dataset.table_specific_name('Possible_values_clean'),
            'attr_name')

        new_df_possible = self.spark_session.createDataFrame(
            possible_values_dirty, StructType([
                StructField("vid", IntegerType(), True),
                StructField("tid", IntegerType(), False),
                StructField("attr_name", StringType(), False),
                StructField("attr_val", StringType(), False),
                StructField("observed", IntegerType(), False),
                StructField("domain_id", IntegerType(), False),
            ])
        )
        self.dataengine.add_db_table('Possible_values_dk',
                                     new_df_possible, self.dataset)
        self.dataengine.add_db_table_index(
            self.dataset.table_specific_name('Possible_values_dk'), 'attr_name')
        del new_df_possible

        # Create Clean and DK flats
        new_df_clean = self.spark_session.createDataFrame(
            c_clean, StructType([
                StructField("tid", IntegerType(), False),
                StructField("attribute", StringType(), False),
                StructField("value", StringType(), True)
            ])
        )
        self.dataengine.add_db_table('C_clean_flat',
                                     new_df_clean, self.dataset)

        new_df_dk = self.spark_session.createDataFrame(
            c_dk, StructType([
                StructField("tid", IntegerType(), False),
                StructField("attribute", StringType(), False),
                StructField("value", StringType(), True)
            ])
        )
        self.dataengine.add_db_table('C_dk_flat',
                                     new_df_dk, self.dataset)

        new_df_kij = self.spark_session.createDataFrame(
            domain_kij_dk, StructType([
                StructField("vid", IntegerType(), True),
                StructField("tid", IntegerType(), False),
                StructField("attr_name", StringType(), False),
                StructField("k_ij", IntegerType(), False),
            ]))
        self.dataengine.add_db_table('Kij_lookup_dk',
                                     new_df_kij, self.dataset)

        new_df_kij = self.spark_session.createDataFrame(
            domain_kij_clean, StructType([
                StructField("vid", IntegerType(), False),
                StructField("tid", IntegerType(), False),
                StructField("attr_name", StringType(), False),
                StructField("k_ij", IntegerType(), False),
            ]))
        self.dataengine.add_db_table('Kij_lookup_clean',
                                     new_df_kij, self.dataset)

        self.dataengine.holo_env.logger.info('The table: ' +
                                            self.dataset.table_specific_name(
                                                'Kij_lookup_clean') +
                                            " has been created")
        self.dataengine.holo_env.logger.info("  ")
        self.dataengine.holo_env.logger.info('The table: ' +
                                            self.dataset.table_specific_name(
                                                'Possible_values_dk') +
                                            " has been created")
        self.dataengine.holo_env.logger.info("  ")

        del new_df_kij
        del new_df_clean
        del new_df_dk

        create_feature_id_map = "Create TABLE " + \
                                self.dataset.table_specific_name(
                                    "Feature_id_map") + \
                                "( feature_ind INT," \
                                " attribute VARCHAR(255)," \
                                " value VARCHAR(255)," \
                                " type VARCHAR(255) );"
        self.dataengine.query(create_feature_id_map)

        query_observed = "CREATE TABLE " + \
                         self.dataset.table_specific_name(
                             'Observed_Possible_values_clean') + \
                         " SELECT * FROM ( " \
                         "SELECT *  \
                         FROM " + \
                         self.dataset.table_specific_name(
                             'Possible_values_clean') + " as t1 " + \
                         " WHERE " \
                         " t1.observed=1 ) " \
                         "AS table1;"

        self.dataengine.query(query_observed)

        query_observed = "CREATE TABLE " + \
                         self.dataset.table_specific_name(
                             'Observed_Possible_values_dk') + \
                         " SELECT * FROM ( " \
                         "SELECT *  \
                         FROM " + \
                         self.dataset.table_specific_name('Possible_values_dk')\
                         + " as t1 " + \
                         " WHERE " \
                         " t1.observed=1 ) " \
                         "AS table1;"

        self.dataengine.query(query_observed)

        self.dataengine.holo_env.logger.info('The table: ' +
                                            self.dataset.table_specific_name(
                                            'Possible_values_dk') +
                                            " has been created")
        self.dataengine.holo_env.logger.info("  ")

        self.dataengine.holo_env.logger.info('The table: ' +
                                            self.dataset.table_specific_name(
                                            'Possible_values_clean') +
                                            " has been created")
        self.dataengine.holo_env.logger.info("  ")
        return
