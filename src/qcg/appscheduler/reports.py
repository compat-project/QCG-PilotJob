import json
import logging
import io


class JobReport:
    def __init__(self, report_file, buffer_size=100):
        self.report_file = report_file
        self.buffer =  io.StringIO()
        self.bufferedEntries = 0
        self.bufferSize = buffer_size

    def reportJob(self, job, iteration):
        self.reportJobEntry(job, iteration, self.buffer)
        self.bufferedEntries += 1

        if self.bufferedEntries > self.bufferSize:
            self.flush()

    def flush(self):
        with open(self.report_file, 'a') as f:
            f.write(self.buffer.getvalue())

        self.buffer.close()
        self.buffer = io.StringIO()
        self.bufferedEntries = 0

    def reportJobEntry(self, job, iteration, ostream):
        raise NotImplementedError()


class TextFileReport(JobReport):
    NAME = 'text'

    def __init__(self, report_file):
        super(TextFileReport, self).__init__(report_file)
        logging.info('initializing TEXT job report')

    def reportJobEntry(self, job, iteration, ostream):
        jname = job.getName(iteration)
        jstate = job.getStateStr(iteration)
        jmessages = job.getMessages(iteration)
        jhistory = job.getHistory(iteration)
        jruntime = job.getRuntime(iteration)

        ostream.write("%s (%s) %s\n\t%s\n\t%s\n" % (jname, jstate, jmessages or '',
                                              "\n\t".join(
                                                  ["{}: {}".format(str(en[1]), en[0].name) for en in jhistory]),
                                              "\n\t".join(
                                                  ["{}: {}".format(k, v) for k, v in jruntime.items()])))


class JsonFileReport(JobReport):
    NAME = 'json'

    def __init__(self, report_file):
        super(JsonFileReport, self).__init__(report_file)
        logging.info('initializing JSON job report')

    def reportJobEntry(self, job, iteration, ostream):
        jname = job.getName(iteration)
        jstate = job.getStateStr(iteration)
        jmessages = job.getMessages(iteration)
        jhistory = job.getHistory(iteration)
        jruntime = job.getRuntime(iteration)

        data = {
            'name': jname,
            'state': jstate,
            'history': [ { 'state': e[0].name, 'date': e[1].isoformat() } for e in jhistory ],
            'runtime': { k: v for k, v in jruntime.items() },
        }

        if iteration is None:
            data['execution'] = job.execution.toDict()
            data['resources'] = job.resources.toDict()

            if job.dependencies:
                data['dependencies'] = job.dependencies.toDict()

        if jmessages:
            data['messages'] = jmessages

        ostream.write(json.dumps(data, separators=(',', ': ')) + '\n')


def none_reporter():
    return None


_available_formats = {
    'none': none_reporter,
    TextFileReport.NAME: TextFileReport,
    JsonFileReport.NAME: JsonFileReport
}


def getReporter(reportFormat, report_file):
    if reportFormat not in _available_formats:
        raise ValueError('reporter {} not available'.format(reportFormat))

    return _available_formats[reportFormat](report_file)
