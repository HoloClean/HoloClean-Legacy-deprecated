import sys 
sys.path.append('../')
import dcerrordetector as dc

class ErrorDetectors:
    def __init__(self,DenialConstraints,dataengine,spark_session,detection_type = None):
        if detection_type is None:
            self.detect_obj=dc.DCErrorDetection(DenialConstraints,dataengine,spark_session)
    def fill_table(self,data_dataframe):
        noisy_cells=self.detect_obj.noisy_cells(data_dataframe)
        clean_cells=self.detect_obj.clean_cells(data_dataframe,noisy_cells)
        return noisy_cells,clean_cells
        
             
        

  