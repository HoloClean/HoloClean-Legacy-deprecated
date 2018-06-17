from ..DCFormatException import DCFormatException


class ParserInterface:
    """
    This class creates interface for parsing denial constraints
    """

    def __init__(self, session):
        """
        Constructing parser interface object

        :param session: session object
        """
        self.session = session
        self.dataengine = session.holo_env.dataengine

    def load_denial_constraints(self, file_path, all_current_dcs):
        """
        Loads denial constraints from line-separated txt file

        :param file_path: path to dc file
        :param all_current_dcs: list of current dcs in the session

        :return: list of Denial Constraint strings and their respective Denial Constraint Objects
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
                denial_constraints[line] = \
                    (DenialConstraint(
                        line,
                        self.session.dataset.attributes['Init']))
        return denial_constraints_strings, denial_constraints


class Predicate:
    """
    This class represents predicates
    """

    def __init__(self, predicate_string, tuple_names, schema):
        """
        Constructing predicate object

        :param predicate_string: string shows the predicate
        :param tuple_names: name of tuples in denial constraint
        :param schema: list of attributes
        """
        self.schema = schema
        self.tuple_names = tuple_names
        self.cnf_form = ""
        op_index = DenialConstraint.contains_operation(predicate_string)
        if op_index is not None:
            self.operation_string = DenialConstraint.operationSign[op_index]
            self.operation = DenialConstraint.operationsArr[op_index]
        else:
            raise DCFormatException('Cannot find Operation in Predicate: ' +
                                    predicate_string)
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
        """
        Parses the components of given predicate string
        Example: 'EQ(t1.ZipCode,t2.ZipCode)' returns [['t1', 'ZipCode'], ['t2','ZipCode']]
        :param predicate_string: predicate string

        :return: list of predicate components
        """

        # HC currently only supports DCs with two tuples per predicate
        # so raise an exception if a different number present
        num_tuples = len(predicate_string.split(','))
        if num_tuples < 2:
            raise DCFormatException('Less than 2 tuples in predicate: ' +
                                    predicate_string)
        elif num_tuples > 2:
            raise DCFormatException('More than 2 tuples in predicate: ' +
                                    predicate_string)

        operation = self.operation_string
        if predicate_string[0:len(operation)] != operation:
            raise \
                DCFormatException('First string in predicate is '
                                  'not operation ' + predicate_string)
        stack = []
        components = []
        current_component = []
        str_so_far = ""
        for i in range(len(operation), len(predicate_string)):
            str_so_far += predicate_string[i]
            if len(stack[-1:]) > 0 and stack[-1] == "'":
                if predicate_string[i] == "'":
                    if i == len(predicate_string) - 1 or \
                            predicate_string[i+1] != ')':
                        raise \
                            DCFormatException(
                                "Expected ) after end of literal"
                            )
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
                    raise DCFormatException(
                        'Closed an unopened (' +
                        predicate_string)
            elif predicate_string[i + 1] == '.':
                if str_so_far in self.tuple_names:
                    current_component.append(str_so_far)
                    str_so_far = ""
                else:
                    raise DCFormatException(
                        'Tuple name ' + str_so_far +
                        ' not defined in ' +
                        predicate_string)

            elif (predicate_string[i + 1] == ',' or
                  predicate_string[i + 1] == ')') and \
                    predicate_string[i] != "'":

                if str_so_far in self.schema:
                    current_component.append(str_so_far)
                    str_so_far = ""
                    components.append(current_component)
                    current_component = []
                else:
                    raise DCFormatException(
                        'Attribute name ' + str_so_far + ' not in schema'
                    )
            elif str_so_far == ',' or str_so_far == '.':
                str_so_far = ''
        return components


class DenialConstraint:
    """
    Class that defines the denial constraints
    """

    operationsArr = ['<>', '<=', '>=', '=', '<', '>', ]
    operationSign = ['IQ', 'LTE', 'GTE', 'EQ', 'LT', 'GT']

    def __init__(self, dc_string, schema):
        """
        Constructing denial constraint object
        This class contains a list of predicates and the tuple_names which define a Denial Constraint

        :param dc_string: string for denial constraint
        :param schema: list of attribute
        """
        dc_string = dc_string.replace('"', "'")
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
            self.predicates.\
                append(Predicate(split[i], self.tuple_names, schema))

        # Create CNF form of the DC
        cnf_forms = [predicate.cnf_form for predicate in self.predicates]
        self.cnf_form = " AND ".join(cnf_forms)
        return

    @staticmethod
    def contains_operation(string):
        """
        Method to check if a given string contains one of the operation signs

        :param string: given string

        :return: operation index in list of pre-defined list of operations or
        Null if string does not contain any
        """
        for i in range(len(DenialConstraint.operationSign)):
            if string.find(DenialConstraint.operationSign[i]) != -1:
                return i
        return None
