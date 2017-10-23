import sys 
sys.path.append('../')
from dcerrordetector import DCErrorDetection


class ErrorDetectors:
    """TODO:
    This class call different error detection method that we needed
    """
    def __init__(self,DenialConstraints,dataengine,spark_session,detection_type = None):
        
        if detection_type is None:
            self.detect_obj=DCErrorDetection(DenialConstraints,dataengine,spark_session)
    
    #Setters:
    
    #Getters:
      
    def get_noisy_dknow_dataframe(self,data_dataframe):
        
        """
        Return tuple of noisy cells and clean cells dataframes             
        :rtype: tuple[spark_dataframe]
        """
        
        noisy_cells=self.detect_obj.get_noisy_cells(data_dataframe)
        clean_cells=self.detect_obj.get_clean_cells(data_dataframe,noisy_cells)
        
        return noisy_cells,clean_cells
 
	         
        

  
