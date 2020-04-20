import math
import logging

from qcg.appscheduler.errors import InvalidRequest

class IterScheduler:

    @classmethod
    def GetScheduler(cls, schedulerName):
        return __SCHEDULERS__.get(schedulerName.lower(), DEFAULT_SCHEDULER)


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
            # all self.iterations will fit at one round
            new_pmax = min(pmax, int(math.floor(self.availResources / (self.iterations))))
            spare, spare_per_iter = 0, 0
            if new_pmax * self.iterations < self.availResources:
                spare = self.availResources - new_pmax * self.iterations
                spare_per_iter = int(math.ceil(float(spare) / self.iterations))

            logging.debug("maximum-iters: for {} tasks scheduled on {} self.availResources the new min & max is ({},{}), spare {}, sper / iter {}".format(
                self.iterations, self.availResources, pmin, new_pmax, spare, spare_per_iter))

            for idx in range(0, self.iterations):
                tmax = new_pmax
                if spare > 0:
                    tmax = min(new_pmax + spare_per_iter, pmax)
                    spare -= tmax - new_pmax

                iterPlan = self.jobResources.copy()
                iterPlan.update({ 'min': pmin, 'max': tmax })
                yield iterPlan
        else:
            # more than one round
            rest = self.iterations
            while rest > 0:
                if rest * pmin > self.availResources:
                    curr_iters = int(math.floor(self.availResources / pmin))
                else:
                    curr_iters = rest

                rest -= curr_iters

#                print("curr_iters: {}, rest: {}".format(curr_iters, rest))

                new_pmax = min(pmax, int(math.floor(self.availResources / (curr_iters))))
                spare, spare_per_iter = 0, 0
                if new_pmax * curr_iters < self.availResources and new_pmax < pmax:
                    spare = self.availResources - new_pmax * curr_iters
                    spare_per_iter = int(math.ceil(float(spare) / curr_iters))

#                print("maximum-iters: for {} tasks the new min & max is ({},{}), spare {}, sper / iter {}".format(
#                    curr_iters, pmin, new_pmax, spare, spare_per_iter))
                logging.debug("maximum-iters: for {} tasks the new min & max is ({},{}), spare {}, sper / iter {}".format(
                    curr_iters, pmin, new_pmax, spare, spare_per_iter))

                for idx in range(0, curr_iters):
                    tmax = new_pmax
                    if spare > 0:
                        tmax = min(new_pmax + spare_per_iter, pmax)
                        spare -= tmax - new_pmax

                    iterPlan = self.jobResources.copy()
                    iterPlan.update({ 'min': pmin, 'max': tmax })
                    yield iterPlan


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
            iterPlan = self.jobResources.copy()
            iterPlan.update({ 'max': splitPart })
            yield iterPlan


DEFAULT_SCHEDULER = MaximumIters

__SCHEDULERS__ = {
    MaximumIters.SCHED_NAME.lower(): MaximumIters,
    SplitInto.SCHED_NAME.lower(): SplitInto,
}
