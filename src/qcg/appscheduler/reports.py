import json
import logging
import io


class JobReport:
    def __init__(self, report_file, buffer_size=100):
        self.report_file = report_file
        self.buffer =  io.StringIO()
        self.bufferedEntries = 0
        self.bufferSize = buffer_size

    def reportJob(self, job):
        self.reportJobEntry(job, self.buffer)
        self.bufferedEntries += 1

        if self.bufferedEntries > self.bufferSize:
            self.flush()

    def flush(self):
        with open(self.report_file, 'a') as f:
            f.write(self.buffer.getvalue())

        self.buffer.close()
        self.buffer = io.StringIO()
        self.bufferedEntries = 0

    def reportJobEntry(self, job, ostream):
        raise NotImplementedError()


class TextFileReport(JobReport):
    NAME = 'text'

    def __init__(self, report_file):
        super(TextFileReport, self).__init__(report_file)
        logging.info('initializing TEXT job report')

    def reportJobEntry(self, job, ostream):
        ostream.write("%s (%s) %s\n\t%s\n\t%s\n" % (job.name, job.strState(), job.messages or '',
                                              "\n\t".join(
                                                  ["{}: {}".format(str(en[1]), en[0].name) for en in job.history]),
                                              "\n\t".join(
                                                  ["{}: {}".format(k, v) for k, v in job.runtime.items()])))


class JsonFileReport(JobReport):
    NAME = 'json'

    def __init__(self, report_file):
        super(JsonFileReport, self).__init__(report_file)
        logging.info('initializing JSON job report')

    def reportJobEntry(self, job, ostream):
        data = {
            'name': job.name,
            'state': job.strState(),
            'history': [ { 'state': e[0].name, 'date': e[1].isoformat() } for e in job.history ],
            'runtime': { k: v for k, v in job.runtime.items() },
            'execution': job.execution.toDict(),
            'resources': job.resources.toDict()
        }
        if job.messages:
            data['messages'] = job.messages
        if job.files:
            data['files'] = job.files.toDict()
        if job.dependencies:
            data['dependencies'] = job.dependencies.toDict()

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
