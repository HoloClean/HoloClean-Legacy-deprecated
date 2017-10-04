

class Featurizer:
    
    @staticmethod
    def signal_features_aggregator(*signals):
        
        """
        Returns join of the input tables
        :type *Dataframes
        :rtype: Dataframe
        """
        numOfSignals=len(signals)
        result=signals[0]
        if numOfSignals >1:
            for i in range(1,numOfSignals):                
                result=result.join(signals[i],'cell')
        return result