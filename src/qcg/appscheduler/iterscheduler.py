import math
import logging

from qcg.appscheduler.errors import InvalidRequest
from qcg.appscheduler.joblist import ResourceSize

class IterScheduler:

    @classmethod
    def GetScheduler(cls, schedulerName):
        return __SCHEDULERS__.get(schedulerName.lower(), DEFAULT_SCHEDULER)

    @staticmethod
    def get_exact_iter_plan(iter_plan, exact):
        target = iter_plan.copy()

        if 'min' in target:
            del target['min']

        if 'max' in target:
            del target['max']

        if 'scheduler' in target:
            del target['scheduler']

        target['exact'] = exact
        return target


class MaximumIters:
    SCHED_NAME = 'maximum-iters'

    def __init__(self, jobResources, iterations, availResources, **params):
        self.jobResources = jobResources
        self.iterations = iterations
        self.availResources = availResources



    def generate(self):
        logging.debug("iteration scheduler '{}' algorithm called".format(MaximumIters.SCHED_NAME))

        pmin = 1
        if 'min' in self.jobResources:
            pmin = self.jobResources['min']

        pmax = 1000000
        if 'max' in self.jobResources:
            pmax = self.jobResources['max']

        if self.iterations * pmin <= self.availResources:
            # a single round
            logging.debug("iterations in single round to schedule: {}, available resources: {}, " +\
                  "minimum iteration resources: {}".format(self.iterations, self.availResources, pmin))

            avail_resources = self.availResources
            for iteration in range(self.iterations):
                # assign part of round_resources to the iteration_in_round
                iteration_resources = math.floor(avail_resources / (self.iterations - iteration))
                avail_resources -= iteration_resources

                logging.debug("iteration: {}/{}, iteration_resources: {}, rest avail_resources: {}".format(
                    iteration, self.iterations, iteration_resources, avail_resources))

                yield IterScheduler.get_exact_iter_plan(self.jobResources.copy(), iteration_resources)
        else:
            # more than one round

            # minimum number of needed rounds
            rounds = math.ceil((self.iterations / math.floor(float(self.availResources) / pmin)))
            iterations_to_schedule = self.iterations

            logging.debug("iterations to schedule: {}, rounds: {}, resources: {}, minimum iteration " +\
                          "resources: {}".format(iterations_to_schedule, rounds, self.availResources, pmin))

            while iterations_to_schedule > 0:
                for round in range(rounds):
                    iterations_in_round = math.ceil(iterations_to_schedule / (rounds - round))
                    round_resources = self.availResources

                    logging.debug("round: {}/{}, iterations_in_round: {}".format(round, rounds, iterations_in_round))

                    for iteration_in_round in range(iterations_in_round):
                        # assign part of round_resources to the iteration_in_round
                        iteration_resources = math.floor(round_resources / (iterations_in_round - iteration_in_round))
                        round_resources -= iteration_resources

                        logging.debug("round: {}/{}, iteration_in_round: {}/{}, iteration_resources: {}, rest " +\
                                      "round_resources: {}".format(round, rounds, iteration_in_round,
                                                                   iterations_in_round, iteration_resources,
                                                                   round_resources))

                        yield IterScheduler.get_exact_iter_plan(self.jobResources.copy(), iteration_resources)

                    iterations_to_schedule -= iterations_in_round
                    logging.debug("end of round: {}/{}, iterations_to_schedule: {}".format(round, rounds, iterations_to_schedule))


class SplitInto:
    SCHED_NAME = 'split-into'

    def __init__(self, jobResources, iterations, availResources, **params):
        self.jobResources = jobResources
        self.iterations = iterations
        self.availResources = availResources

        if 'parts' in params:
            self.splitInto = int(params['parts'])
        else:
            self.splitInto = self.iterations

    def generate(self):
        logging.debug("iteration scheduler '{}' algorithm called".format(SplitInto.SCHED_NAME))

        if 'max' in self.jobResources:
            raise InvalidRequest(
                'Wrong submit request - split-into directive mixed with max directive')

        splitPart = int(math.floor(self.availResources / self.splitInto))
        if splitPart <= 0:
            raise InvalidRequest('Wrong submit request - split-into resolved to zero')

        logging.debug("split-into algorithm of {} self.iterations with {} split-into on {} self.availResources finished with {} splits".format(
            self.iterations, self.splitInto, self.availResources, splitPart))

        for idx in range(0, self.iterations):
            yield IterScheduler.get_exact_iter_plan(self.jobResources.copy(), splitPart)


DEFAULT_SCHEDULER = MaximumIters

__SCHEDULERS__ = {
    MaximumIters.SCHED_NAME.lower(): MaximumIters,
    SplitInto.SCHED_NAME.lower(): SplitInto,
}
