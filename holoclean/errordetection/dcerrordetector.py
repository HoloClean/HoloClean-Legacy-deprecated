from holoclean.utils.dcparser import DCParser
from abstract_errordetector import Abstract_Error_Detection


class DCErrorDetection(Abstract_Error_Detection):
    """
    This class return error
    cells and clean
    cells based on the
    denial constraint
    """

    def __init__(self, DenialConstraints, holo_obj, dataset):
        """
        This constructor at first convert all denial constraints
        to the form of SQL constraints
        and it get dataengine to connect to the database
        :param DenialConstraints: list of denial constraints that use
        :param dataengine: a connector to database
        :param dataset: list of tables name
        :param spark_session: spark session configuration
        """
        super(DCErrorDetection, self).__init__(holo_obj, dataset)
        self.and_of_preds = DCParser(
            DenialConstraints)\
            .get_anded_string('all')

    # Private methods
    def _index2list(self, dataset):
        """
        Returns list of indices
        :rtype: list[string]
        """
        li_tmp = dataset.select('index').collect()

        return [list_count.asDict()['index'] for list_count in li_tmp]

    def _make_cells(self, tuples_dataframe, cond):

        """
        This method create cell based on dataframe it get
        :param tuples_dataframe: spark_dataframe
        :param cond: list[String] of conditions
        :return: spark_dataframe
        """

        all_list = self.dataengine.get_schema(self.dataset, "Init")
        all_list = all_list.split(',')
        attr_list = DCParser.get_attribute(cond, all_list)
        index_data = tuples_dataframe.select('ind')\
            .unionAll(tuples_dataframe.select('indexT2')).distinct()
        dc_data = []
        for attribute in attr_list:
            dc_data.append([attribute])
        dc_df = self.spark_session.createDataFrame(dc_data, ['attr'])

        result = index_data.crossJoin(dc_df)

        return result

    def _violation_tuples(self, dataset):
        """
        Return a list of two column dataframe that consist
        indices that create violation w.r.t. dc
        :param dataset: dataset of tables name
        :return: list[spark_dataframe] list of violations tuples
        """

        dataset.createOrReplaceTempView("df")
        satisfied_tuples_index = []
        nullcells = []
        self.holo_obj.logger.info('Denial Constraint Queries: ')
        for cond in self.and_of_preds:
            query = "SELECT table1.index as ind,table2.index as indexT2 " \
                    "FROM df table1,df table2 " \
                    "WHERE ("+cond+")"
            self.holo_obj.logger.info(query)
            satisfied_tuples_index.append(self.spark_session.sql(query))
        for nullquery in self.null_pred:
            query = "SELECT table1.index as ind,table1.index as\
                indexT2 FROM df table1 WHERE ("+nullquery + ")"
            nullcells.append(self.spark_session.sql(query))

        return satisfied_tuples_index, nullcells

    # Setters

    # Getters

    def get_noisy_cells(self, dataset):
        """
        Return a dataframe that consist of index of noisy cells index,attribute
        :param dataset: list od dataset names
        :return: spark_dataframe
        """

        num_of_constarints = len(self.and_of_preds)
        violation, nullcells = self._violation_tuples(dataset)
        result = self._make_cells(violation[0], self.and_of_preds[0])
        if num_of_constarints > 1:
            for dc_count in range(1, num_of_constarints):
                pred = self.and_of_preds[dc_count]
                result = result.\
                    unionAll(self._make_cells(violation[dc_count], pred))
        return result.distinct()

    def get_clean_cells(self, dataframe, noisy_cells):
        """
        Return a dataframe that consist of index of clean cells index,attribute
        :param dataframe: spark dataframe
        :param noisy_cells: list of noisy cells
        :return:
        """

        dataframe.createOrReplaceTempView("df")
        query = "SELECT table1.index as ind FROM df table1"
        index_set = self.spark_session.sql(query)
        all_attr = self.dataengine.get_schema(self.dataset, "Init").split(',')
        all_attr.remove('index')
        rev_attr_list = []
        for attribute in all_attr:
            rev_attr_list.append([attribute])
        all_attr_df = self.spark_session.\
            createDataFrame(rev_attr_list, ['attr'])
        all_cell = index_set.crossJoin(all_attr_df)

        result = all_cell.subtract(noisy_cells)
        return result
