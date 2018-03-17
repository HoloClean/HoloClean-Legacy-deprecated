from dcparser import DCParser
from ..DCFormatException import DCFormatException


class ParserInterface:

    def __init__(self, session):
        self.session = session
        self.dataengine = session.holo_env.dataengine
        self.tables_name = DCParser.tables_name
        return

    def load_denial_constraints(self, file_path, all_current_dcs):
        """ Loads denial constraints from line-separated txt file
        :param file_path: path to dc file
        :return: string array of dc's
        """
        denial_constraints = self._denial_constraints(file_path,
                                                      all_current_dcs)
        return denial_constraints

    def check_dc_format(self, dc, all_current_dcs):
        """
        determines whether or not the dc is formatted correctly,
        the dc is already in the list and if the attributes match the dataset
        Should be called before adding any DC to session

        :param dc: denial constraint to be checked
        :param dc: all currently added dc's

        :return the dc if dc is correctly formatted
        raises exception if incorrect
        """

        if dc in all_current_dcs:
            raise DCFormatException("Duplicate Denial Constraint")

        split_dc = dc.split('&')

        if len(split_dc) < 3:
            raise DCFormatException("Invalid DC: Missing Information")

        if split_dc[0] != self.tables_name[0] or\
                split_dc[1] != self.tables_name[1]:
            raise DCFormatException("Invalid DC: "
                                    "Tuples Not Defined Correctly")

        operators = DCParser.operationSign

        for inequality in split_dc[2:]:
            split_ie = inequality.split('(')

            if len(split_ie) != 2:
                raise DCFormatException("Invalid DC: "
                                        "Inequality Not Defined Correctly")

            if split_ie[0] == '':
                raise DCFormatException("Invalid DC: "
                                        "Missing Operator")

            if split_ie[0] not in operators:
                raise DCFormatException("Invalid DC: "
                                        "Operator Must Be In " +
                                        str(operators))

            split_tuple = split_ie[1].split(',')
            if len(split_tuple) != 2:
                raise DCFormatException("Invalid DC: "
                                        "Tuple Not Defined Correctly")

        if not self._check_dc_attributes(dc):
            raise DCFormatException("DC uses attribute not in schema")
        return dc

    def get_CNF_of_dcs(self, dcs):
        """
        takes a list of DCs and returns the SQL condition for each DC

        :param dcs: list of string representation of DCs
        :return: returns list of SQL conditions
        """
        dcparser = DCParser(dcs)
        return dcparser.get_anded_string()

    def create_dc_map(self, dcs):
        """
        Returns a dictionary that takes a dc as a string for a key and takes
        a list of its predicates for the value

        Example Output:
        {'(t1.ZipCode=t2.ZipCode)AND(t1.City,t2.City)':
            [
                ['t1.ZipCode= t2.ZipCode', '=','t1.ZipCode',
                't2.ZipCode',0],
                ['t1.City<>t2.City', '<>','t1.City', 't2.City'
                , 0]
            ]
        }

        :param dcs: a list of DC's with their string representation
        :return: A dictionary of mapping every dc to their list of predicates
        """
        dictionary_dc = {}
        for dc in dcs:
            list_preds = self.find_predicates(dc)
            dictionary_dc[dc] = list_preds
        return dictionary_dc

    def find_predicates(self, dc):
        """
        This method finds the predicates of dc"
       
        input example: '(t1.ZipCode=t2.ZipCode)AND(t1.City,t2.City)'
        output example
        [['t1.ZipCode= t2.ZipCode', '=','t1.ZipCode', 't2.ZipCode',0],
        ['t1.City<>t2.City', '<>','t1.City', 't2.City' , 0]]

        :param dc: a string representation of the denial constraint

        :rtype: predicate_list: list of predicates and it's componenents and
                                type:
                        [full predicate string, component 1, component 2,
                        (0=no literal or 1=component 1 is literal or
                        2=component 2 is literal) ]
        """

        predicate_list = []
        operations_list = DCParser.operationsArr
        predicates = dc.split(' AND ')
        components = []
        for predicate in predicates:
            predicate_components = []
            dc_type = 0
            predicate_components.append(predicate)
            for operation in operations_list:
                if operation in predicate:
                    components = predicate.split(operation)
                    predicate_components.append(operation)
                    break

            component_index = 1
            for component in components:
                if component.find(self.tables_name[0] + ".") == -1 and \
                   component.find(self.tables_name[1] + ".") == -1:
                    dc_type = component_index
                predicate_components.append(component)
                component_index = component_index + 1

            predicate_components.append(dc_type)
            predicate_list.append(predicate_components)

        return predicate_list

    def get_dc_attributes(self, dc):
        """
        given a dc, will return a list of attributes used in the DC

        :param dc: string representation of a DC
        :return: list of strings (each string is an attribute name)
        """
        attributes = set()
        predicates = self.find_predicates(dc)
        for predicate in predicates:
            component1 = predicate[2].split('.')
            component2 = predicate[3].split('.')
            dc_type = predicate[4]
            # dc_type 0 : we do not have a literal in this predicate
            # dc_type 1 : we have a literal in the left side of the predicate
            # dc_type 2 : we have a literal in the right side of the predicate
            if dc_type == 1:
                attributes.add(component2[1])
            elif dc_type == 2:
                attributes.add(component1[1])
            else:
                attributes.add(component1[1])
                attributes.add(component2[1])
        return attributes

    def get_all_constraint_attributes(self, dcs):
        """
        Given a list of DC, will return a list of all attributes used
        in all DC's in the list

        :param dcs: list of string representation of DCs
        :return: list of attributes
        """
        dcparser = DCParser(dcs)
        return dcparser.get_constrainted_attributes(self.dataengine,
                                                    self.session.dataset)

    # private methods

    def _check_dc_attributes(self, dc):
        """
        Will check if the attributes used in dc are all in the dataset

        :param dc: String representation of DC
        :return: True if all attributes are in the dataset, False otherwise
        """
        dcparser = DCParser([dc])
        attributes = self.get_dc_attributes(dcparser.get_anded_string()[0])
        schema = DCParser.get_all_attribute(self.dataengine,
                                            self.session.dataset)
        for attribute in attributes:
            if attribute not in schema:
                return False
        return True

    def _denial_constraints(self, filepath, all_current_dcs):
        """
        Read a textfile containing the the Denial Constraints

        :param filepath: The path to the file containing DCs
        :param all_current_dcs: all the current dc's in the list to compare with
        :return: None
        """

        denial_constraints = []
        dc_file = open(filepath, 'r')
        for line in dc_file:
            if not line.isspace():
                line = line.rstrip()
                self.check_dc_format(line, all_current_dcs)
                denial_constraints.append(line)
        return denial_constraints
