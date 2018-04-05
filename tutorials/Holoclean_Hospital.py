

from holoclean.holoclean import HoloClean, Session

holo       = HoloClean(
            holoclean_path="..",         # path to holoclean package
            verbose=False,
            # to limit possible values for training data
            pruning_threshold1=0.1,
            # to limit possible values for training data to less than k values
            pruning_clean_breakoff=6,
            # to limit possible values for dirty data (applied after
            # Threshold 1)
            pruning_threshold2=0,
            # to limit possible values for dirty data to less than k values
            pruning_dk_breakoff=6,
            # learning parameters
            learning_iterations=30,
            learning_rate=0.001,
            batch_size=5
        )
session = Session(holo)

data_path = "data/hospital.csv"

data = session.load_data(data_path)

dc_path = "data/hospital_constraints.txt"

dcs = session.load_denial_constraints(dc_path)


data.select('City').show(15)


from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection

detector = SqlDCErrorDetection(session)

error_detector_list =[]
error_detector_list.append(detector)
clean, dirty = session.detect_errors(error_detector_list)


clean.head(5)


dirty.head(5)


repaired = session.repair()


repaired = repaired.withColumn("__ind", repaired["__ind"].cast("int"))
repaired.sort('__ind').select('City').show(15)



session.compare_to_truth("data/hospital_clean.csv")

