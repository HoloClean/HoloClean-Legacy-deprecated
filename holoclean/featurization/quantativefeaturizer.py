


class QuantativeStatisticsFeaturize:
    
    #For each QuantativeStatisticsFeaturize , data_dataframe and noisy cells are needed
    def __init__(self,data_dataframe):
        self.data_dataframe=data_dataframe


    # This value calculate the mean value of the cell attribute in the table   
    
    
    def average_value (self,Table,cell):
        pass
    
    
    def featurize(self,spark_session):
        
        """
        Creates dataframe of features using various statistics of the data
        :param noisy_data: List((int, string))
        :return: dataframe
        """
        
        feature_table_list = []
        lst_attributes = self.data_dataframe.columns
        indexList=self.index2list()
        
        for i in indexList:
            for attr in lst_attributes:
                
#                 if (i, attr) not in self.noisy_cells and attr!='index':
                if  attr!='index':
                    feature_table_list.append(self.put_value_in_vector(i,attr,self.indexValue(i, attr)))
        
        new_df = spark_session.createDataFrame(feature_table_list)
        return new_df
    
    """
    Number of times the value v appeared in the data_dataframe under attribute attr
    """
    def frequency(self , attr, v):
        
        """
        Return number repetition of attribute with given value 
        :param attr: string
        :param v
        :return: int
        """
        count= len(self.data_dataframe.filter(self.data_dataframe[attr]==v).collect())
        return count
    
    
    """
    Within one row, count the number of times both the attr1 has the value v 
    and the attr2 has value w
    """
    def frequency2(self, attr1, attr2, row_index):

        v = self.indexValue(row_index, attr1)
        w = self.indexValue(row_index, attr2)
    
        count= len(self.data_dataframe.filter(self.data_dataframe[attr1]==v).filter(self.data_dataframe[attr2]==w).collect())
        return count
    
    
    """
    Within one row, count the number of times both the attr1 has the value v 
    and the attr2 has value w and the attr3 has value u
    """
    def frequency3(self, attr1, attr2, attr3, row_index):
        v = self.indexValue(row_index, attr1)
        w = self.indexValue(row_index, attr2)
        u = self.indexValue(row_index, attr3)
    
        count= len(self.data_dataframe.filter(self.data_dataframe[attr1]==v).filter(self.data_dataframe[attr2]==w).filter(self.data_dataframe[attr3]==u).collect())
        return count
    
    
    def put_value_in_vector(self,index,attr,v):
        
        """
        This function get all statistical value and put the in the feature vector
        :param index: int
        :param attr:  string
        :param v: Nat
        :return: list
        """
        
        lst_attributes=self.data_dataframe.columns
        new_row = {}
        new_row['cell'] = (index, attr)
        for a in lst_attributes:
            if a!='index':
                title = 'freq_' + str(a)
                if a == attr:
                    new_row[title] = self.frequency(attr, v)
                else:
                    new_row[title] = 0
    
        for a1, a2 in itertools.combinations(lst_attributes, 2):
            if a1!='index' and a2!='index' :
                title = 'freq_2_' + a1 + "_" + a2
                if attr == a1 or attr == a2:
                    new_row[title] = self.frequency2( a1, a2, index)
                else:
                    new_row[title] = 0
    
        for a1, a2, a3 in itertools.combinations(lst_attributes, 3):
            if a1!='index' and a2!='index' and a3!='index' :
                title = 'freq_3_' + a1 + "_" + a2 + "_" + a3
                if attr == a1 or attr == a2 or attr == a3:
                    new_row[title] = self.frequency3(a1, a2, a3, index)
                else:
                    new_row[title] = 0
        return new_row

    def indexValue(self,ind,attr):
        """
        Returns the value of cell with index=ind and column=attr
        :type ind: int
        :type attr: string
        :rtype: list[list[None]]
        """
        row=self.data_dataframe.filter(self.data_dataframe['index']==ind).collect()
        row2dict=row[0].asDict()
        
        return row2dict[attr]
    
    def index2list(self):
        li_tmp=self.data_dataframe.select('index').collect()
        
        return [i.asDict()['index'] for i in li_tmp ]