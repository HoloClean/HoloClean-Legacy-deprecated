from pyspark.sql.types import *

class RandomVar:
    """TODO:RandomVar class: class for random variable"""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Pruning:
    """TODO:Pruning class: Creates the domain table for all the cells"""

    def __init__(self, dataengine, dataset, spark_session, threshold=0.5):
        """TODO.
                Parameters
                --------
                spark_session:Takes as an argument the spark_Session
                threshold:The threshold that we will use for the pruning
                dataengine:Takes as an argument the Data Engine to create Domain table
                """
        self.spark_session = spark_session
        self.dataengine = dataengine
        self.threshold = threshold
        self.dataset = dataset
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

        self.noisycells = self._d_cell()
        self.cellvalues = self._c_values()
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
        dataframe_dont_know = self.dataengine.get_table_to_dataframe("C_dk", self.dataset)
        noisy_cells = []
        self.noisy_list = []
        for cell in dataframe_dont_know.collect():
            cell_variable = RandomVar(columnname=cell[1], row_id=int(cell[0]))
            noisy_cells.append(cell_variable)
            self.noisy_list.append([cell[1], int(cell[0])])

        return noisy_cells

    def _c_values(self):
        """
                Create c_value list from the init table
        """
        dataframe_init = self.dataengine.get_table_to_dataframe("Init", self.dataset)
        table_attribute = dataframe_init.columns
        row_id = 0
        cell_values = {}
        number_id = 0
        for column in dataframe_init.drop('index').collect():
            row = {}
            column_id = 1
            for column_value in column:
                cell_variable = RandomVar(columnname=table_attribute[column_id],
                                          value=column_value, tupleid=row_id, cellid=number_id)
                row[column_id] = cell_variable
                number_id = number_id + 1
                column_id = column_id + 1
            cell_values[row_id] = row
            row_id = row_id + 1
        return cell_values

    def _compute_number_of_coocurences(self, original_attribute, original_attr_value, cooccured_attribute,
                                       cooccured_attr_value):
        """
        generate_assignments creates assignment for each cell with the attribute and value
                    of each other cell in the same row
                    Parameters
        :param original_attribute: the name of first attribute
        :param original_attr_value: the initial value of the first attribute
        :param cooccured_attribute: the name of second attribute
        :param cooccured_attr_value: the initial value of the second attribute
        :return:
        """
        if (original_attr_value, cooccured_attr_value) not in \
                self.domain_pair_stats[original_attribute][cooccured_attribute]:
            return None
        cooccur_count = self.domain_pair_stats[original_attribute][cooccured_attribute][(original_attr_value,
                                                                                         cooccured_attr_value)]
        v_cnt = self.domain_stats[original_attribute][original_attr_value]

        # Compute counter
        number_of_cooccurence = cooccur_count  # / len(self.cellvalues)
        total_number_of_original_attr_value = v_cnt  # / len(self.cellvalues)
        return number_of_cooccurence / total_number_of_original_attr_value

    def _find_domain(self, assignment, trgt_attr):
        """TO DO: _find_domain finds the domain for each cell
                Parameters
                --------
                assignment: attributes with value
                trgt_attr: the name of attribute
        """
        cell_values = {(assignment[trgt_attr])}
        for attr in assignment:
            if attr == trgt_attr:
                continue
            attr_val = assignment[attr]
            if attr in self.cooocurance_for_first_attribute:
                if attr_val in self.cooocurance_for_first_attribute[attr]:
                    if trgt_attr in self.cooocurance_for_first_attribute[attr][attr_val]:
                        cell_values |= set(
                            self.cooocurance_for_first_attribute[attr][attr_val][trgt_attr].keys())

        return cell_values

    def _preprop(self):
        """TO DO:
               preprocessing phase. create the dictionary with all the attributes.
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

        # This part adds other (dirty) attributes to the dictionary of the key attribute
        for column_name_key in self.column_to_col_index_dict:
            self.domain_pair_stats[column_name_key] = {}
            for col2 in self.dirty_cells_attributes:
                if col2 != column_name_key:
                    self.domain_pair_stats[column_name_key][col2] = {}
        return

    def _analyze_entries(self):
        """TO DO:
                analyzeEntries creates a dictionary with occurrences of the attributes
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
                    # This part adds all cells that has attribute with dc violation to be prunned
                    self.all_cells.append(cell)
                self.all_cells_temp[cell.cellid] = cell

                if val not in self.domain_stats[col]:
                    self.domain_stats[col][val] = 0.0
                self.domain_stats[col][val] += 1.0

            # Iterate over target attributes and grab counts of values with other attributes
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
        for original_attribute in self.domain_pair_stats:  # For each column in the cooccurences
            self.cooocurance_for_first_attribute[original_attribute] = {}  # It creates a dictionary
            for cooccured_attribute in self.domain_pair_stats[original_attribute]:
                # For second column in the cooccurences Over
                # Pair of values that happend with each other
                # (original_attribute value , cooccured_attribute value)
                for assgn_tuple in self.domain_pair_stats[original_attribute][cooccured_attribute]:
                    cooccure_number = self._compute_number_of_coocurences(
                        original_attribute, assgn_tuple[0], cooccured_attribute, assgn_tuple[1])
                    if cooccure_number > self.threshold:
                        if assgn_tuple[0] not in self.cooocurance_for_first_attribute[original_attribute]:
                            self.cooocurance_for_first_attribute[original_attribute][assgn_tuple[0]] = {}
                        if cooccured_attribute not in \
                                self.cooocurance_for_first_attribute[original_attribute][assgn_tuple[0]]:
                            self.cooocurance_for_first_attribute[original_attribute][
                                assgn_tuple[0]][cooccured_attribute] = {}
                        self.cooocurance_for_first_attribute[original_attribute][assgn_tuple[0]][cooccured_attribute][
                            assgn_tuple[1]] = cooccure_number
        return

    def _generate_assignments(self):
        """
        generate_assignments creates assignment for each cell with the attribute and value
                of each other cell in the same row
        :return:
        """
        for cell in self.all_cells:
            tplid = cell.tupleid
            trgt_attr = cell.columnname

            # assignment is a dictionary for each cell copied the row of that cell
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
            # In this part we get all values for cell_index's attribute_to_be_pruned
            self.cell_domain[cell_index] = self._find_domain(
                self.assignments[cell_index], self.attribute_to_be_pruned[cell_index])
        return

    def _create_dataframe(self):
        """
        creates a spark dataframe from cell_domain for all the cells
        :return:
        """
        attributes = self.dataengine.get_schema(self.dataset, 'Init').split(',');
        domain_dict={}
        for attribute in attributes:
            if attribute != 'index' and attribute != 'Index':
                domain_dict[attribute] = set([])

        list_to_dataframe_possible_values = []
        list_to_dataframe_init = []
        for tuple_id in self.cellvalues:
            for cell_index in self.cellvalues[tuple_id]:
                attribute = self.cellvalues[tuple_id][cell_index].columnname
                value = self.cellvalues[tuple_id][cell_index].value
                domain_dict[attribute].add(value)

                list_to_dataframe_init.append([(self.cellvalues[tuple_id][cell_index].tupleid + 1),
                                               self.cellvalues[tuple_id][cell_index].columnname,
                                               unicode(self.cellvalues[tuple_id][cell_index].value)])

                # If the value is in the id of cell in the involve attribute we put as not observed
                tmp_cell_index = self.cellvalues[tuple_id][cell_index].cellid
                if tmp_cell_index in self.cell_domain:
                    for value in self.cell_domain[tmp_cell_index]:
                        if value != self.all_cells_temp[tmp_cell_index].value:
                            list_to_dataframe_possible_values.append(
                                                                     [(self.all_cells_temp[tmp_cell_index].tupleid + 1),
                                                                      self.all_cells_temp[tmp_cell_index].columnname,
                                                                      unicode(value), "0", 'String' if isinstance(value, unicode) or isinstance(value, str) else 'Number'])
                        else:
                            list_to_dataframe_possible_values.append(
                                                                     [(self.all_cells_temp[tmp_cell_index].tupleid + 1),
                                                                      self.all_cells_temp[tmp_cell_index].columnname,
                                                                      unicode(value), "1", 'String' if isinstance(value, unicode) or isinstance(value, str) else 'Number'])
        # Create possible table
        new_df_possible = self.spark_session.createDataFrame(
            list_to_dataframe_possible_values, [
                'tid', 'attr_name', 'attr_val', 'observed', 'data_type'])
        self.dataengine.add_db_table('Possible_values',
                                     new_df_possible, self.dataset)

        # Create Initial table in flatted view
        new_df_init = self.spark_session.createDataFrame(
            list_to_dataframe_init, ['tid', 'attr_name', 'attr_val'])
        self.dataengine.add_db_table('Init_flat',
                                     new_df_init, self.dataset)

        # Create dataframe for Domain Map
        max_domain = 0
        for attribute in domain_dict:
            max_domain = len(domain_dict[attribute]) if len(domain_dict[attribute]) > max_domain else max_domain
        for attribute in domain_dict:
            while len(domain_dict[attribute]) < max_domain:
                domain_dict[attribute].add('*')
        list_domain_map = []
        index = 1
        for attribute in domain_dict:
            value_index = 1
            for value in domain_dict[attribute]:
                list_domain_map.append([index, self.dataengine.attribute_map[attribute], value_index, str(value)])
                value_index = value_index + 1
                index = index + 1

        # Send dataframe to Domain_Map Table
        df_domain_map = self.spark_session.createDataFrame(
            list_domain_map, StructType([
                StructField("index", IntegerType(), False),
                StructField("attr_index", IntegerType(), True),
                StructField("val_index", IntegerType(), True),
                StructField("value", StringType(), True),
            ]))
        self.dataengine.add_db_table('Domain_Map',
                                     df_domain_map, self.dataset)


        query_for_create_offset = "CREATE TABLE \
            " + self.dataset.table_specific_name('offset') \
            + "(Feature Text,offset INT);"
        self.dataengine.query(query_for_create_offset)

        insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name(
            'offset') + " (Feature, offset) Values ('Init',1);"
        self.dataengine.query(insert_signal_query)

        insert_signal_query = "INSERT INTO " + self.dataset.table_specific_name(
            'offset') + " (Feature, offset) Values ('Coocur',"+str(len(list_domain_map)) +");"
        self.dataengine.query(insert_signal_query)
        return
