from qcg.appscheduler.api.errors import *


JOB_TOP_ATTRS = {
    "exec":    { 'req': True,  'types': [ str ]       },
    "args":    { 'req': False, 'types': [ list, str ] },
    "stdin":   { 'req': False, 'types': [ str ]       },
    "stdout":  { 'req': False, 'types': [ str ]       },
    "stderr":  { 'req': False, 'types': [ str ]       },
    "nodes":   { 'req': False, 'types': [ dict ]      },
    "cores":   { 'req': False, 'types': [ dict ]      },
    "wt":      { 'req': False, 'types': [ str ]       },
    "iterate": { 'req': False, 'types': [ dict ]      },
    "after":   { 'req': False, 'types': [ list, str ] }
}

# resources (nodes, cores) attributes
JOB_RES_ATTRS = {
    "min":      { 'req': False,  'types': [ int ] },
    "max":      { 'req': False,  'types': [ int ] },
    "exact":    { 'req': False,  'types': [ int ] },
    "fraction": { 'req': False,  'types': [ int ] }
}

# iteration attributes
JOB_IT_ATTRS = {
    "start": { 'req': False,  'types': [ int ] },
    "stop":  { 'req': False,  'types': [ int ] },
    "steps": { 'req': False,  'types': [ int ] }
}


class Jobs:
    """
    Group of job descriptions to submit

    Attributes:
        list (dict) - list of jobs
    """

    def __init__(self):
        self.__list = {}


    def __validateJob(self, **attrs):
        for attr in attrs:
            if attr not in JOB_TOP_ATTRS:
                raise InvalidJobDescription("Unknown attribute '%s'" % attr)

            typeValid = False
            for t in JOB_TOP_ATTRS[attr]:
                if isinstance(attrs[attr], t):
                    typeValid = True
                    break

            if not typeValid:
                raise InvalidJobDescription("Invalid attribute '%s' type '%" % (attr, type(attrs[attr])))

        for reqAttr in JOB_TOP_ATTRS:
            if JOB_TOP_ATTRS[reqAttr]['req'] and reqAttr not in attrs:
                raise InvalidJobDescription("Required attribute '%s' not defined" % reqAttr)

        for res in [ 'nodes', 'cores' ]:
            if res in attrs:
                for nattr in attrs[res]:
                    if nattr not in JOB_RES_ATTRS:
                        raise InvalidJobDescription("Unknown attribute %s->'%s'" % (res, nattr))

                    typeValid = False
                    for t in JOB_RES_ATTRS[nattr]:
                        if isinstance(attrs[res][nattr], t):
                            typeValid = True
                            break

                if not typeValid:
                    raise InvalidJobDescription("Invalid attribute %s->'%s' type '%" % (res, nattr, type(attrs[res][attr])))

                for reqAttr in JOB_RES_ATTRS:
                    if JOB_RES_ATTRS[reqAttr]['req'] and reqAttr not in attrs[res]:
                        raise InvalidJobDescription("Required attribute %s->'%s' not defined" % (res, reqAttr))



    """
    Add a new job to the group.
    
    Args:
        name (str) - unique (among the group) name of the job
        attrs (dict) - job attributes
    """
    def add(self, name, **attrs):
        if name in self.__list:
            raise InvalidJobDescription("Job %s already in list" % name)

        self.__validateJob(self, attrs)

        self.__list[name] = attrs


    """
    Remote a job from the group.
    
    Args:
        name (str) - name of the job to remove
    """
    def remove(self, name):
        if name not in self.__list:
            raise JobNotDefined(name)

        del self.__list[name]