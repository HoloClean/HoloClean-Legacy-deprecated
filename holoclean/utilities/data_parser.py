class ConstraintParser:
     acceptable_rule_expression=[] # we can initialize values with some regular expression for our acceptable code
     def __init__(self,path_to_constraint_file):
         self.path_to_constraint_file=path_to_constraint_file
     #this function take the file and check the semantic and validity of rules 
     def constraint_validity(self):
         pass
     #this function create list of denial constraints
     def constraint_to_denial_constraint(self):
         pass
     #this function return a list of the rules that has error with our parsing logic rule
     def show_false_rules(self):
          pass
     #this function return a list of the rules that true with our parsing logic rule
     def show_true_rules(self):
          pass
     def set_regular_expression(self,new_rules):
          self.acceptable_rule_expression=new_rules
          pass
     def update_regular_expression(self,new_rules):
          pass
class DatasetParser:
     #class for object that parse dataset
     
    def data_format(self,dataset_path):
        splited=dataset_path.split('.')
        return splited[-1]
     
class FeedbackParser:
     """This parser parse the user feedback"""
     
     def read_feedback(self):
          pass
