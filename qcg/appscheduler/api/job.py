import json

from qcg.appscheduler.api.errors import *


# top level job description attributes
JOB_TOP_ATTRS = {
    "name":    { 'req': True,  'types': [ str ]       },
    "exec":    { 'req': True,  'types': [ str ]       },
    "args":    { 'req': False, 'types': [ list, str ] },
    "stdin":   { 'req': False, 'types': [ str ]       },
    "stdout":  { 'req': False, 'types': [ str ]       },
    "stderr":  { 'req': False, 'types': [ str ]       },
    "wd":      { 'req': False, 'types': [ str ]       },
    "numNodes":{ 'req': False, 'types': [ dict ]      },
    "numCores":{ 'req': False, 'types': [ dict ]      },
    "wt":      { 'req': False, 'types': [ str ]       },
    "iterate": { 'req': False, 'types': [ list ]      },
    "after":   { 'req': False, 'types': [ list, str ] }
}

# resources (nodes, cores) attributes of job description
JOB_RES_ATTRS = {
    "min":        { 'req': False,  'types': [ int ] },
    "max":        { 'req': False,  'types': [ int ] },
    "exact":      { 'req': False,  'types': [ int ] },
    "split-into": { 'req': False,  'types': [ int ] },
    "scheduler":  { 'req': False,  'types': [ str ] }
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
        InvalidJobDescriptionError - in case of invalid job description
    """
    def __validateSmplJob(self, attrs):
        if 'name' not in attrs:
            raise InvalidJobDescriptionError("Missing job name")

        if attrs['name'] in self.__list:
            raise InvalidJobDescriptionError("Job %s already in list" % attrs['name'])

        for attr in attrs:
            if attr not in JOB_TOP_ATTRS:
                raise InvalidJobDescriptionError("Unknown attribute '%s'" % attr)

            typeValid = False
            for t in JOB_TOP_ATTRS[attr]['types']:
                if isinstance(attrs[attr], t):
                    typeValid = True
                    break

            if not typeValid:
                raise InvalidJobDescriptionError("Invalid attribute '%s' type '%" % (attr, type(attrs[attr])))

        for reqAttr in JOB_TOP_ATTRS:
            if JOB_TOP_ATTRS[reqAttr]['req'] and reqAttr not in attrs:
                raise InvalidJobDescriptionError("Required attribute '%s' not defined" % reqAttr)

        for res in [ 'numNodes', 'numCores' ]:
            if res in attrs:
                for nattr in attrs[res]:
                    if nattr not in JOB_RES_ATTRS:
                        raise InvalidJobDescriptionError("Unknown attribute %s->'%s'" % (res, nattr))

                    typeValid = False
                    for t in JOB_RES_ATTRS[nattr]['types']:
                        if isinstance(attrs[res][nattr], t):
                            typeValid = True
                            break

                if not typeValid:
                    raise InvalidJobDescriptionError("Invalid attribute %s->'%s' type '%" % (res, nattr, type(attrs[res][attr])))

                for reqAttr in JOB_RES_ATTRS:
                    if JOB_RES_ATTRS[reqAttr]['req'] and reqAttr not in attrs[res]:
                        raise InvalidJobDescriptionError("Required attribute %s->'%s' not defined" % (res, reqAttr))

        if 'iterate' in attrs:
            if len(attrs['iterate']) < 2 or len(attrs['iterate']) > 3:
                raise InvalidJobDescriptionError("The iterate must contain 2 or 3 element list")


    """
    Perform simple validation of a job in format acceptable (StdJob) by the QCG-PJM

    Args:
        stdJob (dict) - job description

    Raises:
        InvalidJobDescriptionError - in case of invalid job description
    """
    def __validateStdJob(self, stdJob):
        if 'name' not in stdJob:
            raise InvalidJobDescriptionError('Missing "name" key')

        if 'execution' not in stdJob or 'exec' not in stdJob['execution']:
            raise InvalidJobDescriptionError('Missing "execution/exec" key')

        if stdJob['name'] in self.__list:
            raise InvalidJobDescriptionError("Job %s already in list" % (stdJob['name']))


    """
    Convert simple job description to a standard format.

    Args:
        jName (str) - a job name
        smplJob (dict) - simple job description

    Returns:
        dict - simple job description
    """
    def __convertSimpleToStd(self, smplJob):
        stdJob = {}

        stdJob['name'] = smplJob['name']
        stdJob['execution'] = {
            'exec': smplJob['exec']
        }

        for key in [ 'args', 'stdin', 'stdout', 'stderr', 'wd' ]:
            if key in smplJob:
                stdJob['execution'][key] = smplJob[key]

        resources = { }
        for mKey in [ 'numCores', 'numNodes' ]:
            if mKey in smplJob:
                resources[mKey] = smplJob[mKey]

        for rKey in [ 'wt' ]:
            if rKey in smplJob:
                resources[rKey] = smplJob[rKey]

        if len(resources) > 0:
            stdJob['resources'] = resources

        for key in [ 'iterate' ]:
            if key in smplJob:
                stdJob[key] = smplJob[key]

        if 'after' in smplJob:
            stdJob['dependencies'] = { 'after': smplJob['after'] }

        return stdJob


    """
    Convert standard job description to a simple format.

    Args:
        stdJob (dict) - standard job description

    Returns:
        dict - simple job description
    """
    def convertStdToSimple(self, stdJob):
        smplJob = { }

        name = stdJob['name']

        for key in [ 'iterate', 'after' ]:
            if key in stdJob:
                smplJob[key] = stdJob[key]

        for eKey in [ 'exec', 'args', 'stdin', 'stdout', 'stderr', 'wd' ]:
            if eKey in stdJob['execution']:
                smplJob[eKey] = stdJob['execution'][eKey]

        if 'resources' in stdJob:
            for rKey in [ 'numCores', 'numNodes', 'wt' ]:
                if rKey in stdJob['resources']:
                    smplJob[rKey] = stdJob['resources'][rKey]

        if 'dependencies' in stdJob and 'after' in stdJob['dependencies']:
            smplJob['after'] = stdJob['dependencies']['after']

        return (name, smplJob)


    """
    Add a new, simple job description to the group.
    If both arguments are present, they are merged and processed as a single dictionary.
    
    Args:
        dAttrs (dict) - attributes as a dictionary in a simple format
        stdAttrs (dict) - attributes as a named arguments in a simple format

    Raises:
        InvalidJobDescriptionError - in case of non-unique job name or invalid job description
    """
    def add(self, dAttrs = None, **attrs):
        data = attrs

        if dAttrs is not None:
            if data is not None:
                data = { **dAttrs, **data }
            else:
                data = dAttrs

        self.__validateSmplJob(data)
        self.__list[data['name']] = self.__convertSimpleToStd(data)

        return self


    """
    Add a new, standard job description (acceptable by the QCG PJM) to the group.
    If both arguments are present, they are merged and processed as a single dictionary.

    Args:
        dAttrs (dict) - attributes as a dictionary in a standard format
        stdAttrs (dict) - attributes as a named arguments in a standard format

    Raises:
        InvalidJobDescriptionError - in case of non-unique job name or invalid job description

    """
    def addStd(self, dAttrs = None, **stdAttrs):
        data = stdAttrs

        if dAttrs is not None:
            if data is not None:
                data = { **dAttrs, **data }
            else:
                data = dAttrs

        self.__validateStdJob(data)
        self.__list[data['name']] = data

        return self


    """
    Remote a job from the group.
    
    Args:
        name (str) - name of the job to remove

    Raises:
        JobNotDefinedError - in case of missing job in a group with given name
    """
    def remove(self, name):
        if name not in self.__list:
            raise JobNotDefinedError(name)

        del self.__list[name]


    """
    Return a list with job names in grup.
    """
    def jobNames(self):
        return list(self.__list)


    """
    Return job descriptions in format acceptable by the QCG-PJM

    Returns:
        list - a list of jobs in the format acceptable by the QCG PJM (standard format)
    """
    def jobs(self):
        return list(self.__list.values())


    """
    Read job's descriptions from JSON file in format acceptable (StdJob) by the QCG-PJM

    Args:
        filePath (str) - path to the file with jobs descriptions in a standard format

    Raises:
        InvalidJobDescriptionError - in case of invalid job description
    """
    def loadFromFile(self, filePath):
        try:
            with open(filePath, 'r') as f:
                for job in json.load(f):
                    self.__validateStdJob(job)

                    name = job['name']
                    if name in self.__list:
                        raise InvalidJobDescriptionError('Job "%s" already defined"' % (name))

                    self.__list[name] = job
        except QCGPJMAError as qe:
            raise qe
        except Exception as e:
            raise FileError('File to open/write file "%s": %s', filePath, e.args[0])


    """
    Save job list to JSON file in a standard format.

    Args:
        filePath (str) - path to the destination file

    Raises:
        FileError - in case of problems with opening / writing output file.
    """
    def saveToFile(self, filePath):
        try:
            with open(filePath, 'w') as f:
                f.write(json.dumps(self.jobs(), indent=2))
        except Exception as e:
            raise FileError('File to open/write file "%s": %s', filepath, e.args[0])
