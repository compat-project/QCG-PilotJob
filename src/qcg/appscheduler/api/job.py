import json
import uuid

from qcg.appscheduler.api.errors import *


# top level job description attributes
JOB_TOP_ATTRS = {
    "name":    { 'req': True,  'types': [ str ]       },
    "exec":    { 'req': False, 'types': [ str ]       },
    "script":  { 'req': False, 'types': [ str ]       },
    "args":    { 'req': False, 'types': [ list, str ] },
    "stdin":   { 'req': False, 'types': [ str ]       },
    "stdout":  { 'req': False, 'types': [ str ]       },
    "stderr":  { 'req': False, 'types': [ str ]       },
    "wd":      { 'req': False, 'types': [ str ]       },
    "modules": { 'req': False, 'types': [ list, str ] },
    "venv":    { 'req': False, 'types': [ str ]       },
    "numNodes":{ 'req': False, 'types': [ int, dict ] },
    "numCores":{ 'req': False, 'types': [ int, dict ] },
    "wt":      { 'req': False, 'types': [ str ]       },
    "iterate": { 'req': False, 'types': [ int, dict ] },
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

ITERATE_ATTRS = {
    "start":     { 'req': False, 'types': [ int ]  },
    "stop":      { 'req': True,  'types': [ int ]  }
}

MAX_ITERATIONS = 64 * 1024


class Jobs:

    def __init__(self):
        """
        Group of job descriptions to submit
        """
        self.__list = {}
        self.__jobIdx = 0


    def __generate_default_name(self):
        return str(uuid.uuid4())


    def __validateSmplJob(self, attrs):
        """
        Validate job description attributes.
        It's is not a full validation, only the attributes names and types are checked.

        Args:
            attrs (dict) - job description attributes

        Raises:
            InvalidJobDescriptionError - in case of invalid job description
        """
        if 'name' not in attrs:
            attrs['name'] = self.__generate_default_name()

        if attrs['name'] in self.__list:
            raise InvalidJobDescriptionError("Job {} already in list".format(attrs['name']))

        for attr in attrs:
            if attr not in JOB_TOP_ATTRS:
                raise InvalidJobDescriptionError("Unknown attribute {}".format(attr))

            typeValid = False
            for t in JOB_TOP_ATTRS[attr]['types']:
                if isinstance(attrs[attr], t):
                    typeValid = True
                    break

            if not typeValid:
                raise InvalidJobDescriptionError("Invalid attribute {} type {}".format(attr, type(attrs[attr])))

        if 'after' in attrs and isinstance(attrs['after'], str):
            # convert after string to single element list
            attrs['after'] = [ attrs['after'] ]

        for reqAttr in JOB_TOP_ATTRS:
            if JOB_TOP_ATTRS[reqAttr]['req'] and reqAttr not in attrs:
                raise InvalidJobDescriptionError("Required attribute '%s' not defined" % reqAttr)

        if not 'exec' in attrs and not 'script' in attrs:
            raise InvalidJobDescriptionError("No 'exec' nor 'script' defined")

        if 'exec' in attrs and 'script' in attrs:
            raise InvalidJobDescriptionError("Both 'exec' and 'script' defined")

        if 'numCores' in attrs and isinstance(attrs['numCores'], int):
            attrs['numCores'] = { 'exact': attrs['numCores'] }

        if 'numNodes' in attrs and isinstance(attrs['numNodes'], int):
            attrs['numNodes'] = { 'exact': attrs['numNodes'] }

        self.__validateSmplElementDefinition(['numNodes', 'numCores'], attrs, JOB_RES_ATTRS)

        if 'iterate' in attrs:
            if isinstance(attrs['iterate'], int):
                attrs['iterate'] = {'start': 0, 'stop': attrs['iterate']}

            self.__validateSmplElementDefinition(['iterate'], attrs, ITERATE_ATTRS)

            iter_start = attrs['iterate']['start'] if 'start' in attrs['iterate'] else 0
            niters = attrs['iterate']['stop'] - iter_start

            if niters < 0 or niters > MAX_ITERATIONS:
                raise InvalidJobDescriptionError("Wrong number of iterations - outside range (0, {})".format(
                    MAX_ITERATIONS))


    def __validateSmplElementDefinition(self, element_names, element_attributes, element_definition):
        for element_name in element_names:
            if element_name in element_attributes:
                for element_attr in element_attributes[element_name]:
                    if element_attr not in element_definition:
                        raise InvalidJobDescriptionError("Unknown {} attribute {}".format(element_name, element_attr))

                    typeValid = False
                    for t in element_definition[element_attr]['types']:
                        if isinstance(element_attributes[element_name][element_attr], t):
                            typeValid = True
                            break

                if not typeValid:
                    raise InvalidJobDescriptionError("Invalid {} attribute {} type {}".format(
                        element_name, element_attr, type(element_attributes[element_name][element_attr])))

                for req_attr in element_definition:
                    if element_definition[req_attr]['req'] and req_attr not in element_attributes[element_name]:
                        raise InvalidJobDescriptionError("Required attribute {} of element {} not defined".format(
                            req_attr, element_name))


    def __validateStdJob(self, stdJob):
        """
        Perform simple validation of a job in format acceptable (StdJob) by the QCG-PJM

        Args:
            stdJob (dict) - job description

        Raises:
            InvalidJobDescriptionError - in case of invalid job description
        """
        if 'name' not in stdJob:
            stdJob['name'] = self.__generate_default_name()

        if 'execution' not in stdJob or ('exec' not in stdJob['execution'] and 'script' not in stdJob['execution']):
            raise InvalidJobDescriptionError('Missing "execution/exec" key')

        if stdJob['name'] in self.__list:
            raise InvalidJobDescriptionError("Job {} already in list".format(stdJob['name']))


    def __convertSimpleToStd(self, smplJob):
        """
        Convert simple job description to a standard format.

        Args:
            jName (str) - a job name
            smplJob (dict) - simple job description

        Returns:
            dict - simple job description
        """
        stdJob = {}

        stdJob['name'] = smplJob['name']
        if 'exec' in smplJob:
            stdJob.setdefault('execution', {})['exec'] = smplJob['exec']
        else:
            stdJob.setdefault('execution', {})['script'] = smplJob['script']

        for key in [ 'args', 'stdin', 'stdout', 'stderr', 'wd', 'modules', 'venv' ]:
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
                stdJob['iteration'] = smplJob[key]

        if 'after' in smplJob:
            stdJob['dependencies'] = { 'after': smplJob['after'] }

        return stdJob


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
        self.__appendJob(data['name'], self.__convertSimpleToStd(data))

        return self


    def __appendJob(self, jName, jData):
        """
        Append a new job to internal list.
        All jobs should be appended by this function, which also put each of the job the index,
        which will be used to return ordered list of jobs.

        Args:
            jName (str) - job name
            jData (dict) - job attributes in standard format
        """
        self.__list[jName] = { 'idx': self.__jobIdx, 'data': jData }
        self.__jobIdx += 1


    def addStd(self, dAttrs = None, **stdAttrs):
        """
        Add a new, standard job description (acceptable by the QCG PJM) to the group.
        If both arguments are present, they are merged and processed as a single dictionary.

        Args:
            dAttrs (dict) - attributes as a dictionary in a standard format
            stdAttrs (dict) - attributes as a named arguments in a standard format

        Raises:
            InvalidJobDescriptionError - in case of non-unique job name or invalid job description

        """
        data = stdAttrs

        if dAttrs is not None:
            if data is not None:
                data = { **dAttrs, **data }
            else:
                data = dAttrs

        self.__validateStdJob(data)
        self.__appendJob(data['name'], data)

        return self


    def remove(self, name):
        """
        Remote a job from the group.
        
        Args:
            name (str) - name of the job to remove

        Raises:
            JobNotDefinedError - in case of missing job in a group with given name
        """
        if name not in self.__list:
            raise JobNotDefinedError(name)

        del self.__list[name]


    def clear(self):
        """
        Remove all jobs from the group.

        Returns:
            int - number of removed elements
        """
        njobs = len(self.__list)
        self.__list.clear()
        return njobs


    def jobNames(self):
        """
        Return a list with job names in grup.
        """
        return list(self.__list)


    def orderedJobNames(self):
        """
        Return a list with job names in group in order they were appended.
        """
        return [j[0] for j in sorted(list(self.__list.items()), key=lambda j: j[1]['idx'])]


    def jobs(self):
        """
        Return job descriptions in format acceptable by the QCG-PJM

        Returns:
            list - a list of jobs in the format acceptable by the QCG PJM (standard format)
        """
        return [j['data'] for j in list(self.__list.values())]


    def orderedJobs(self):
        """
        Return job descriptions in format acceptable by the QCG-PJM in order they were appended.

        Returns:
            list - a list of jobs in the format acceptable by the QCG PJM (standard format)
        """
        return [j['data'] for j in sorted(list(self.__list.values()), key=lambda j: j['idx'])]


    def loadFromFile(self, filePath):
        """
        Read job's descriptions from JSON file in format acceptable (StdJob) by the QCG-PJM

        Args:
            filePath (str) - path to the file with jobs descriptions in a standard format

        Raises:
            InvalidJobDescriptionError - in case of invalid job description
        """
        try:
            with open(filePath, 'r') as f:
                for job in json.load(f):
                    self.__validateStdJob(job)

                    name = job['name']
                    if name in self.__list:
                        raise InvalidJobDescriptionError('Job "{}" already defined"'.format(name))

                    self.__appendJob(name, job)
        except QCGPJMAError as qe:
            raise qe
        except Exception as e:
            raise FileError('File to open/write file "{}": {}'.format(filePath, e.args[0]))


    def saveToFile(self, filePath):
        """
        Save job list to JSON file in a standard format.

        Args:
            filePath (str) - path to the destination file

        Raises:
            FileError - in case of problems with opening / writing output file.
        """
        try:
            with open(filePath, 'w') as f:
                f.write(json.dumps(self.orderedJobs(), indent=2))
        except Exception as e:
            raise FileError('File to open/write file "%s": %s', filePath, e.args[0])
