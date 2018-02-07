import math
import random


class Accu:
    def __init__(
            self,
            object_truth,
            source_observations,
            dataengine,
            dataset,
            spark_session):
        # load data

        self.object_truth = object_truth
        self.source_observations = source_observations
        self.spark_session = spark_session
        self.dataset = dataset
        self.dataengine = dataengine

        # init local dictionaries
        self.object_inferred_truth = {}
        self.object_distinct_observations = {}
        self.object_observations = {}
        # object to value dictionary
        for source_id in self.source_observations:
            for oid in self.source_observations[source_id]:
                if oid in object_truth:
                    continue
                if oid not in self.object_observations:
                    self.object_observations[oid] = []
                    self.object_distinct_observations[oid] = set([])
                    self.object_inferred_truth[oid] = source_observations[source_id][oid]
                self.object_observations[oid].append(
                    (source_id, source_observations[source_id][oid]))
                self.object_distinct_observations[oid].add(
                    source_observations[source_id][oid])

        # initialize source accuracy --- utilize any ground truth if specified
        self.source_accuracy = {}
        self._init_src_accuracy()

    def _init_src_accuracy(self):
        for source_id in self.source_observations:
            correct = 0.0
            total = 0.0
            for oid in self.source_observations[source_id]:
                if oid in self.object_truth:
                    total += 1.0
                    self.object_inferred_truth[oid] = self.object_truth[oid]
                    if self.object_truth[oid] == self.source_observations[source_id][oid]:
                        correct += 1.0
            if total == 0.0:
                self.source_accuracy[source_id] = round(
                    random.uniform(0.5, 1), 3)
            else:
                self.source_accuracy[source_id] = correct / total
            if self.source_accuracy[source_id] == 1.0:
                self.source_accuracy[source_id] = 0.99
            elif self.source_accuracy[source_id] == 0.0:
                self.source_accuracy[source_id] = 0.01

    def update_object_assignment(self):
        for oid in self.object_observations:
            obs_scores = {}
            for (src_id, value) in self.object_observations[oid]:
                if value not in obs_scores:
                    obs_scores[value] = 0.0
                if len(self.object_distinct_observations[oid]) == 1:
                    obs_scores[value] = 1.0
                else:
                    obs_scores[value] += math.log((len(self.object_distinct_observations[oid]) - 1)
                                                  * self.source_accuracy[src_id] / (1 - self.source_accuracy[src_id]))

            # assign largest score
            self.object_inferred_truth[oid] = max(
                obs_scores, key=obs_scores.get)
            return

    def update_source_accuracy(self):
        for source_id in self.source_observations:
            correct = 0.0
            total = 0.0
            for oid in self.source_observations[source_id]:
                if oid in self.object_inferred_truth:
                    total += 1.0
                    if self.object_inferred_truth[oid] == self.source_observations[source_id][oid]:
                        correct += 1.0
            assert total != 0.0
            self.source_accuracy[source_id] = correct / total
            if self.source_accuracy[source_id] == 1.0:
                self.source_accuracy[source_id] = 0.99
            elif self.source_accuracy[source_id] == 0.0:
                self.source_accuracy[source_id] = 0.01

    def solve(self, iterations=10):
        for i in range(iterations):
            if i % 1000 == 0:
                print i
            self.update_object_assignment()
            self.update_source_accuracy()
        self.create_dataframe()
        return

    def create_dataframe(self):
        object_list = []
        i=0
        print "Print weight_accuracy for the first 10 sources:"
        for accu in self.source_accuracy:
            print accu,self.source_accuracy[accu]
            i = i+1
            if i == 10:
                break
        for object in self.object_inferred_truth:
            object_name = object.split('!_!')
            assigned_val = self.object_inferred_truth[object]
            object_list.append([object_name[0], object_name[1], assigned_val])

        new_df_possible = self.spark_session.createDataFrame(
            object_list, [
                'rv_index', 'rv_attr', 'assigned_val'])
        self.dataengine.add_db_table('Final',
                                     new_df_possible, self.dataset)
