import os
import json

from json import JSONDecodeError


def save_jobs_to_file(jobs, file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

    with open(file_path, 'w') as f:
        f.write(json.dumps([ job.toDict() for job in jobs ], indent=2))


def save_reqs_to_file(reqs, file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

    with open(file_path, 'w') as f:
        f.write(json.dumps(reqs, indent=2))


def check_job_status_in_json(jobs, workdir='.', dest_state='SUCCEED'):
    to_check = set(jobs)

    report_path = os.path.join(workdir, 'jobs.report')
    with open(report_path, 'r') as report_f:
        for line, entry in enumerate(report_f, 1):
            try:
                job_entry = json.loads(entry)
            except JSONDecodeError as e:
                raise ValueError('wrong jobs.report {} file format: error in {} line: {}'.format(report_path, line, str(e)))

            for attr in [ 'name', 'state' ]:
                if not attr in job_entry:
                    raise ValueError('wrong jobs.report {} file format: missing \'{}\' attribute'.format(report_path, attr))

            jName = job_entry['name']
            state = job_entry['state']

            if jName in to_check:
                msg = job_entry.get('message', None)
                if state != dest_state:
                    raise ValueError('job\'s {} incorrect status \'{}\' vs expected \'{}\' {}'.format(jName, state, dest_state,
                        '(' + msg  + ')' if msg else ''))

                to_check.remove(jName)

            if len(to_check) == 0:
                break

    if len(to_check) > 0:
        raise ValueError('missing {} jobs in report'.format(','.join(to_check)))