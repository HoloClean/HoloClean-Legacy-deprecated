import random
from holoclean.global_variables import GlobalVariables
import time


class RandomVar:
    """RandomVar class: class for random variable"""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Pruning:
    """Pruning class: Creates the domain table for all the cells"""

    def __init__(self, session, threshold1=0.0, threshold2=0.3, breakoff=3):
        """

            :param session: Holoclean session
            :param threshold: the threshold will use for pruning
        """
        self.session = session
        self.spark_session = session.holo_env.spark_session
        self.dataengine = session.holo_env.dataengine
        self.threshold1 = threshold1
        self.threshold2 = threshold2
        self.breakoff = breakoff
        self.dataset = session.dataset
        self.assignments = {}
        self.cell_domain_nb = {}
        self.domain_stats = {}
        self.domain_pair_stats = {}
        self.column_to_col_index_dict = {}
        self.attribute_to_be_pruned = {}
        self.dirty_cells_attributes = set([])
        self.coocurence_lookup = {}
        self.cell_domain = {}
        self.all_cells = []
        self.all_cells_temp = {}
        self.index = 0

        self.cellvalues = self._c_values()
        self.noisycells = self._d_cell()
        t1 =time.time()
        self._preprop()
        t2 =time.time()
        if session.holo_env.verbose:
            session.holo_env.logger.info("_preprop " + str(t2-t1))

        self._analyze_entries()
        t3 =time.time()
        if session.holo_env.verbose:
            session.holo_env.logger.info("_analyze_entries " + str(t3-t2))

        self._generate_assignments()
        t4 =time.time()
        if session.holo_env.verbose:
            session.holo_env.logger.info("_generate_assignments " + str(t4-t3))

        self._generate_coocurences()
        t5 =time.time()
        if session.holo_env.verbose:
            session.holo_env.logger.info("_generate_coocurences " + str(t5-t4))

        self._find_cell_domain()
        t6 =time.time()
        if session.holo_env.verbose:
            session.holo_env.logger.info("_find_cell_domain " + str(t6-t5))

        self._create_dataframe()
        t7 = time.time()
        if session.holo_env.verbose:
            session.holo_env.logger.info("_create_dataframe " + str(t7-t6))

    # Internal Method
    def _d_cell(self):
        """
                Create noisy_cell list from the C_dk table
        """
        dataframe_dont_know = self.session.dk_df
        noisy_cells = []
        for cell in dataframe_dont_know.collect():
            cell_variable = RandomVar(columnname=cell[1], row_id=int(cell[0]))
            noisy_cells.append(cell_variable)
            self.cellvalues[int(cell[0])-1][self.attribute_map[cell[1]]].dirty = 1

        return noisy_cells



    def _c_values(self):
        """
                Create c_value list from the init table
        """
        dataframe_init = \
            self.session.init_dataset
        table_attribute = dataframe_init.columns
        row_id = 0
        cell_values = {}
        self.attribute_map = {}
        number_id = 0

        for record in dataframe_init.drop(GlobalVariables.index_name).collect():
            # for each record creates a new row
            row = {}
            column_id = 0
            for column_value in record:
                # For each column
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
        value_count = self.domain_stats[original_attribute][original_attr_value]

        # Compute counter

        if original_attr_value is None or cooccured_attr_value is None:
            probability = 0
        else:
            probability = cooccur_count/ value_count
        return probability


    # we need a new function like find_domain for clean cells
    # such that it does not limit the domain to the possible values
    # above the threshold
    # first iteration, use a low threshold (i.e. 0) and limit using k


    def _find_dk_domain(self, assignment, trgt_attr):
        """ This method finds the domain for each dirty cell for inference

           :param assignment: the values for every attribute
           :param trgt_attr: the name of attribute
        """
        # cell_probabilities will hold domain values and their probabilities
        cell_probabilities = []
        cell_values = {(assignment[trgt_attr])}
        for attr in assignment:
            if attr == trgt_attr:
                continue
            attr_val = assignment[attr]

            if attr in self.coocurence_lookup:
                if attr_val in self.coocurence_lookup[attr]:
                    if trgt_attr in self.coocurence_lookup[attr][attr_val]:
                        if trgt_attr in self.coocurence_lookup[attr][attr_val]:
                            cell_probabilities += \
                                [(k,v) for k,v in self.coocurence_lookup[attr][attr_val][trgt_attr].iteritems()]

        # sort cell_values and chop after k and chop below threshold2
        cell_probabilities.sort(key=lambda t: t[1], reverse=True)
        for tuple in cell_probabilities:
            value = tuple[0]
            probability = tuple[1]
            if len(cell_values) == self.breakoff or probability < self.threshold2:
                break
            cell_values.add(value)
        return cell_values


    def _find_clean_domain (self, assignment, trgt_attr):
        """ This method finds the domain for each clean cell for learning

             :param assignment: the values for every attribute
             :param trgt_attr: the name of attribute
        """
        cell_probabilities = []
        cell_values = {(assignment[trgt_attr])}
        for attr in assignment:
            if attr == trgt_attr:
                continue
            attr_val = assignment[attr]

            if attr in self.coocurence_lookup:
                if attr_val in self.coocurence_lookup[attr]:
                    if trgt_attr in self.coocurence_lookup[attr][attr_val]:
                        if trgt_attr in self.coocurence_lookup[attr][attr_val]:
                            cell_probabilities += \
                                [(k, v) for k, v in self.coocurence_lookup[attr][attr_val][trgt_attr].iteritems()]

        # first iteration
        # get l values from the lookup exactly  like in dirty where l < k
        # get k-l random once from the domain
        cell_probabilities.sort(key=lambda t: t[1])
        while len(cell_values) < self.breakoff/2 and len(cell_probabilities) > 0:  # for now l = k/2
            tuple = cell_probabilities.pop()
            value = tuple[0]
            cell_values.add(value)
        while len(cell_probabilities) > 0 and len(cell_values) < self.breakoff:
            random.shuffle(cell_probabilities)
            tuple = cell_probabilities.pop()
            value = tuple[0]
            cell_values.add(value)
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
            self.coocurence_lookup[original_attribute] = {}
            # It creates a dictionary
            for cooccured_attribute in self.domain_pair_stats[original_attribute]:
                # For second column in the cooccurences Over
                # Pair of values that happened with each other
                # (original_attribute value , cooccured_attribute value)
                for assgn_tuple in self.domain_pair_stats[
                                        original_attribute][
                                        cooccured_attribute]:
                    co_prob = self._compute_number_of_coocurences(
                        original_attribute, assgn_tuple[0], cooccured_attribute,
                        assgn_tuple[1])

                    if co_prob > self.threshold1:
                            if assgn_tuple[0] not in \
                                    self.coocurence_lookup[
                                        original_attribute]:
                                self.coocurence_lookup[
                                    original_attribute][assgn_tuple[0]] = {}

                            if cooccured_attribute not in \
                                    self.coocurence_lookup[
                                        original_attribute][assgn_tuple[0]]:
                                self.coocurence_lookup[
                                    original_attribute][
                                    assgn_tuple[0]][cooccured_attribute] = {}

                            self.coocurence_lookup[
                                original_attribute][assgn_tuple[0]][
                                cooccured_attribute][
                                assgn_tuple[1]] = co_prob


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
            self.assignments[cell] = assignment
            self.attribute_to_be_pruned[cell.cellid] = trgt_attr
        return

    def _find_cell_domain(self):
        """
        find_cell_domain finds the domain for each cell
        :return:
        """
        for cell in self.assignments:
            # In this part we get all values for cell_index's
            # attribute_to_be_pruned

            # if the cell is dirty call find domain
            # else, call get negative examples (domain for clean cells)
            if cell.dirty == 1:
                self.cell_domain[cell.cellid] = self._find_dk_domain(
                    self.assignments[cell],
                    self.attribute_to_be_pruned[cell.cellid])
            else:
                self.cell_domain[cell.cellid] = self._find_clean_domain(
                    self.assignments[cell],
                    self.attribute_to_be_pruned[cell.cellid])
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

        attributes = self.dataset.get_schema('Init')
        domain_dict = {}
        domain_kij_clean = []
        domain_kij_dk = []
        for attribute in attributes:
            if attribute != GlobalVariables.index_name:
                domain_dict[attribute] = set()

        possible_values_clean = []
        possible_values_dirty = []
        self.v_id_clean_list = []
        self.v_id_dk_list = []
        v_id_clean = v_id_dk = 0

        self.assignments = None
        self.attribute_to_be_pruned = None
        self.attribute_map = None

        self.simplepredictions = []

        for tuple_id in self.cellvalues:
            for cell_index in self.cellvalues[tuple_id]:
                attribute = self.cellvalues[tuple_id][cell_index].columnname
                value = self.cellvalues[tuple_id][cell_index].value
                domain_dict[attribute].add(value)

                if self.cellvalues[tuple_id][cell_index].dirty == 1:
                    tmp_cell_index = \
                        self.cellvalues[tuple_id][cell_index].cellid
                    if self.cellvalues[tuple_id][cell_index].domain == 1:

                        if len(self.cell_domain[tmp_cell_index]) == 1:
                            self.simplepredictions.append(
                                [
                                    None,  # vid
                                    tuple_id + 1,  # tid
                                    attribute,  # attr_name
                                    list(self.cell_domain[tmp_cell_index])[0],  # attr_val
                                    1,  # observed
                                    None  # domain_id
                                ]
                            )
                        else:
                            k_ij = 0
                            v_id_dk = v_id_dk + 1

                            self.v_id_dk_list.append([(self.all_cells_temp[
                                                      tmp_cell_index].tupleid
                                                  + 1),
                                                 self.all_cells_temp[
                                                     tmp_cell_index].columnname,
                                                  tmp_cell_index])
                            for value in self.cell_domain[tmp_cell_index]:
                                if value != ():
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
                    tmp_cell_index = \
                        self.cellvalues[tuple_id][cell_index].cellid
                    if self.cellvalues[tuple_id][cell_index].domain == 1:
                        k_ij = 0
                        v_id_clean = v_id_clean + 1
                        self.v_id_clean_list.append([(self.all_cells_temp[
                                                  tmp_cell_index].tupleid
                                              + 1),
                                             self.all_cells_temp[
                                                 tmp_cell_index].columnname, tmp_cell_index])
                        for value in self.cell_domain[tmp_cell_index]:
                              if value != 0:
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

        self.all_cells = None
        self.all_cells_temp = None

        # Create possible table
        new_df_possible = self.spark_session.createDataFrame(
            possible_values_clean, self.dataset.attributes['Possible_values']
        )

        self.dataengine.add_db_table('Possible_values_clean',
                                     new_df_possible, self.dataset)
        self.dataengine.add_db_table_index(
            self.dataset.table_specific_name('Possible_values_clean'),
            'attr_name')
        
        new_df_possible_dk = self.spark_session.createDataFrame(
            possible_values_dirty, self.dataset.attributes['Possible_values']
        )

        self.dataengine.add_db_table('Possible_values_dk',
                                     new_df_possible_dk, self.dataset)
        self.dataengine.add_db_table_index(
            self.dataset.table_specific_name('Possible_values_dk'), 'attr_name')

        new_df_kij = self.spark_session.createDataFrame(
            domain_kij_dk, self.dataset.attributes['Kij_lookup'])
        self.dataengine.add_db_table('Kij_lookup_dk',
                                     new_df_kij, self.dataset)

        new_df_kij = self.spark_session.createDataFrame(
            domain_kij_clean, self.dataset.attributes['Kij_lookup'])
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
                         " AS SELECT * FROM ( " \
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
                         "  AS SELECT * FROM ( " \
                         "SELECT *  \
                         FROM " + \
                         self.dataset.table_specific_name('Possible_values_dk')\
                         + " as t1 " + \
                         " WHERE " \
                         " t1.observed=1 ) " \
                         "AS table1;"
        self.dataengine.query(query_observed)
        new_df_simple_predictions = self.spark_session.createDataFrame(
            self.simplepredictions, self.dataset.attributes['Possible_values']
        )
        self.session.simple_predictions = new_df_simple_predictions
        self.dataengine.add_db_table('Observed_Possible_values_dk',
                                     new_df_simple_predictions, self.dataset, append=1)
        return
