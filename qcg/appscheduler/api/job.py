from qcg.appscheduler.api.errors import *


# top level job description attributes
JOB_TOP_ATTRS = {
    "exec":    { 'req': True,  'types': [ str ]       },
    "args":    { 'req': False, 'types': [ list, str ] },
    "stdin":   { 'req': False, 'types': [ str ]       },
    "stdout":  { 'req': False, 'types': [ str ]       },
    "stderr":  { 'req': False, 'types': [ str ]       },
    "wd":      { 'req': False, 'types': [ str ]       },
    "nodes":   { 'req': False, 'types': [ dict ]      },
    "cores":   { 'req': False, 'types': [ dict ]      },
    "wt":      { 'req': False, 'types': [ str ]       },
    "iterate": { 'req': False, 'types': [ list ]      },
    "after":   { 'req': False, 'types': [ list, str ] }
}

# resources (nodes, cores) attributes of job description
JOB_RES_ATTRS = {
    "min":        { 'req': False,  'types': [ int ] },
    "max":        { 'req': False,  'types': [ int ] },
    "exact":      { 'req': False,  'types': [ int ] },
    "split-into": { 'req': False,  'types': [ int ] }
}


class Jobs:
    """
    Group of job descriptions to submit
    """

    def __init__(self):
        self.__list = {}


    """
    Validate job description attributes.
    It's is not a full validation, only the attributes names and types are checked.

    Args:
        attrs (dict) - job description attributes

    Raises:
        InvalidJobDescription - in case of invalid job description
    """
    def __validateJob(self, attrs):
        for attr in attrs:
            if attr not in JOB_TOP_ATTRS:
                raise InvalidJobDescription("Unknown attribute '%s'" % attr)

            typeValid = False
            for t in JOB_TOP_ATTRS[attr]['types']:
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

        if 'iterate' in attrs:
            if len(attrs['iterate']) < 2 or len(attrs['iterate']) > 3:
                raise InvalidJobDescription("The iterate must contain 2 or 3 element list")


    """
    Add a new job description to the group.
    
    Args:
        name (str) - unique (among the group) name of the job
        attrs (dict) - job attributes

    Raises:
        InvalidJobDescription - in case of non-unique job name or invalid job description
    """
    def add(self, name, attrs):
        if name in self.__list:
            raise InvalidJobDescription("Job %s already in list" % name)

        self.__validateJob(attrs)

        self.__list[name] = attrs

        return self


    """
    Remote a job from the group.
    
    Args:
        name (str) - name of the job to remove

    Raises:
        JobNotDefined - in case of missing job in a group with given name
    """
    def remove(self, name):
        if name not in self.__list:
            raise JobNotDefined(name)

        del self.__list[name]


    """
    Return a list with job names in grup.
    """
    def jobNames(self):
        return list(self.__list)


    """
    Return job descriptions in format acceptable by the QCG-PJM
    """
    def formatDoc(self):
        resJobs = [ ]

        for jName, job in self.__list.items():
            resJob = {}

            resJob['name'] = jName
            resJob['execution'] = {
                'exec': job['exec']
            }

            for key in [ 'args', 'stdin', 'stdout', 'stderr', 'wd' ]:
                if key in job:
                    resJob['execution'][key] = job[key]

            resources = { }
            for mKey in [ { 'name': 'cores', 'alias': 'numCores' },
                          { 'name': 'nodes', 'alias': 'numNodes' } ]:
                if mKey['name'] in job:
                    resources[mKey['alias']] = { } 

                    for key in [ 'min', 'max', 'exact', 'split-into' ]:
                        if key in job:
                            resources[mKey['alias']][key] = job[key]

            if len(resources) > 0:
                resJob['resources'] = resources

            if 'iterate' in job:
                resJob['iterate'] = job['iterate']

            resJobs.append(resJob)

        return resJobs
