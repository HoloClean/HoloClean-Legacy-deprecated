from dcparser import DCParser
from ..DCFormatException import DCFormatException


class ParserInterface:

    def __init__(self, session):
        self.session = session
        self.dataengine = session.holo_env.dataengine
        self.tables_name = DCParser.tables_name
        return

    def load_denial_constraints(self, file_path, all_current_dcs):
        """Loads denial constraints from line-separated txt file

        :param file_path: path to dc file
        :param all_current_dcs: list of current dcs in the session
        :return: string array of dc's
        """
        denial_constraints_strings = []
        denial_constraints = {}
        dc_file = open(file_path, 'r')
        for line in dc_file:
            if not line.isspace():
                line = line.rstrip()
                if line in all_current_dcs:
                    raise DCFormatException('DC already added')
                denial_constraints_strings.append(line)
                denial_constraints[line] = (DenialConstraint(line, self.session.dataset.attributes['Init']))
        return denial_constraints_strings, denial_constraints

    def get_CNF_of_dcs(self, dcs):
        """
        takes a list of DCs and returns the SQL condition for each DC

        :param dcs: list of string representation of DCs
        :return: returns list of SQL conditions
        """
        dcparser = DCParser(dcs)
        example = dcparser.get_anded_string()
        return example

    def create_dc_map(self, dc_list):
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

        :param dc_list: a list of DC's with their string representation
        :return: A dictionary of mapping every dc to their list of predicates
        """
        dcs = self.get_CNF_of_dcs(dc_list)
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


class Predicate:

    def __init__(self, predicate_string, tuple_names, schema):
        self.schema = schema
        self.tuple_names = tuple_names
        self.cnf_form = ""
        op_index = DenialConstraint.contains_operation(predicate_string)
        if op_index is not None:
            self.operation_string = DenialConstraint.operationSign[op_index]
            self.operation = DenialConstraint.operationsArr[op_index]
        else:
            raise DCFormatException('Cannot find Operation in Predicate: ' + predicate_string)
        self.components = self.parse_components(predicate_string)

        for i in range(len(self.components)):
            component = self.components[i]
            if isinstance(component, str):
                self.cnf_form += component
            else:
                self.cnf_form += component[0] + "." + component[1]
            if i < len(self.components) - 1:
                self.cnf_form += self.operation

        return

    def parse_components(self, predicate_string):
        operation = self.operation_string
        if predicate_string[0:len(operation)] != operation:
            raise DCFormatException('First string in predicate is not operation ' + predicate_string)
        stack = []
        components = []
        current_component = []
        str_so_far = ""
        for i in range(len(operation), len(predicate_string)):
            str_so_far += predicate_string[i]
            if len(stack[-1:]) > 0 and stack[-1] == "'":
                if predicate_string[i] == "'":
                    if i == len(predicate_string) - 1 or predicate_string[i+1] != ')':
                        raise DCFormatException("Expected ) after end of literal")
                    components.append(str_so_far)
                    current_component = []
                    stack.pop()
                    str_so_far = ""
            elif str_so_far == "'":
                stack.append("'")
            elif str_so_far == '(':
                str_so_far = ''
                stack.append('(')
            elif str_so_far == ')':
                if stack.pop() == '(':
                    str_so_far = ''
                    if len(stack) == 0:
                        break
                else:
                    raise DCFormatException('Closed an unopened (' + predicate_string)
            elif str_so_far == ',' or str_so_far == '.':
                str_so_far = ''
            elif predicate_string[i + 1] == '.':
                if str_so_far in self.tuple_names:
                    current_component.append(str_so_far)
                    str_so_far = ""
                else:
                    raise DCFormatException('Tuple name ' + str_so_far + ' not defined in ') + predicate_string
            elif (predicate_string[i + 1] == ',' or predicate_string[i + 1] == ')')\
                    and predicate_string[i] != "'":
                if str_so_far in self.schema:
                    current_component.append(str_so_far)
                    str_so_far = ""
                    components.append(current_component)
                    current_component = []
                else:
                    raise DCFormatException('Attribute name ' + str_so_far + ' not in schema')
        return components


class DenialConstraint:

    operationsArr = ['<>', '<=', '>=', '=', '<', '>', ]
    operationSign = ['IQ', 'LTE', 'GTE', 'EQ', 'LT', 'GT']

    def __init__(self, dc_string, schema):
        split = dc_string.split('&')
        self.tuple_names = []
        self.predicates = []
        self.cnf_form = ""

        # Find all tuple names used in DC
        for component in split:
            if DenialConstraint.contains_operation(component) is not None:
                break
            else:
                self.tuple_names.append(component)

        # Make a predicate for each component that's not a tuple name
        for i in range(len(self.tuple_names), len(split)):
            self.predicates.append(Predicate(split[i], self.tuple_names, schema))

        # Create CNF form of the DC
        for predicate in self.predicates:
            self.cnf_form += predicate.cnf_form
            self.cnf_form += " AND "
        self.cnf_form = self.cnf_form[:-5]  # remove AND and spaces at the end
        return

    @staticmethod
    def contains_operation(string):
        for i in range(len(DenialConstraint.operationSign)):
            if string.find(DenialConstraint.operationSign[i]) != -1:
                return i
        return None
