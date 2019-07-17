import math
import logging

from qcg.appscheduler.errors import InvalidRequest

class IterScheduler:

    @classmethod
    def GetScheduler(cls, schedulerName):
        return __SCHEDULERS__.get(schedulerName, __DEFAULT_SCHEDULER__)


class MaximumIters:
    SCHED_NAME = 'maximum-iters'

    @classmethod
    def Schedule(cls, taskRes, iterations, resources):
        logging.debug("iteration scheduler '{}' algorithm called".format(MaximumIters.SCHED_NAME))

        plans = [ ]

        pmin = 1
        if 'min' in taskRes:
            pmin = taskRes['min']

        pmax = 1000000
        if 'max' in taskRes:
            pmax = taskRes['max']

        if iterations * pmin <= resources:
            # all iterations will fit at one round
            new_pmax = min(pmax, int(math.floor(resources / (iterations))))
            spare, spare_per_iter = 0, 0
            if new_pmax * iterations < resources:
                spare = resources - new_pmax * iterations
                spare_per_iter = int(math.ceil(float(spare) / iterations))

            logging.debug("maximum-iters: for {} tasks scheduled on {} resources the new min & max is ({},{}), spare {}, sper / iter {}".format(
                iterations, resources, pmin, new_pmax, spare, spare_per_iter))

            for idx in range(0, iterations):
                tmax = new_pmax
                if spare > 0:
                    tmax = min(new_pmax + spare_per_iter, pmax)
                    spare -= tmax - new_pmax

                iterPlan = taskRes.copy()
                iterPlan.update({ 'min': pmin, 'max': tmax })
                plans.append(iterPlan)
        else:
            # more than one round
            rest = iterations
            while rest > 0:
                if rest * pmin > resources:
                    curr_iters = int(math.floor(resources / pmin))
                else:
                    curr_iters = rest

                rest -= curr_iters

                new_pmax = min(pmax, int(math.floor(resources / (curr_iters))))
                spare, spare_per_iter = 0, 0
                if new_pmax * curr_iters < resources and new_pmax < pmax:
                    spare = resources - new_pmax * curr_iters
                    spare_per_iter = int(math.ceil(float(spare) / curr_iters))

                logging.debug("maximum-iters: for {} tasks the new min & max is ({},{}), spare {}, sper / iter {}".format(
                    curr_iters, pmin, new_pmax, spare, spare_per_iter))

                for idx in range(0, curr_iters):
                    tmax = new_pmax
                    if spare > 0:
                        tmax = min(new_pmax + spare_per_iter, pmax)
                        spare -= new_pmax - new_pmax

                    iterPlan = taskRes.copy()
                    iterPlan.update({ 'min': pmin, 'max': tmax })
                    plans.append(iterPlan)

        return plans





class SplitInto:
    SCHED_NAME = 'split-into'

    @classmethod
    def Schedule(cls, taskRes, iterations, resources):
        logging.debug("iteration scheduler '{}' algorithm called".format(SplitInto.SCHED_NAME))

        if 'split-into' not in taskRes:
            raise InvalidRequest('Missing split-into argument')

        if 'max' in taskRes:
            raise InvalidRequest(
                'Wrong submit request - split-into directive mixed with max directive')

        splitInto = taskRes['split-into']
        if not isinstance(splitInto, int) or splitInto <= 0:
            raise InvalidRequest('Wrong submit request - wrong format of nodes split-into directive')

        splitPart = int(math.floor(resources / splitInto))
        if splitPart <= 0:
            raise InvalidRequest('Wrong submit request - split-into resolved to zero')

        logging.debug("split-into algorithm of {} iterations with {} split-into on {} resources finished with {} splits".format(
            iterations, taskRes['split-into'], resources, splitPart))

        del taskRes['split-into']

        plans = [ ]
        for idx in range(0, iterations):
            iterPlan = { 'max': splitPart }
            iterPlan.update(taskRes)
            plans.append(iterPlan)

        return plans


__DEFAULT_SCHEDULER__ = MaximumIters

__SCHEDULERS__ = {
    MaximumIters.SCHED_NAME: MaximumIters,
    SplitInto.SCHED_NAME: SplitInto,
}
