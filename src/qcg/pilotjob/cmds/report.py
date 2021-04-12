import click
import sys

from qcg.pilotjob.utils.auxdir import find_log_files, find_report_files, find_rtimes_files
from qcg.pilotjob.utils.reportstats import JobsReportStats


def print_stats(stats):
    jstats = stats.job_stats()

    if jstats:
        print('{} jobs executed in {} secs'.format(len(jstats.get('jobs', {})),
            jstats['total_time'].total_seconds() if jstats.get('total_time') else 0))
        print('\t{:>20}: {}'.format('first job queued', str(jstats.get('first_queue', 0))))
        print('\t{:>20}: {}'.format('last job queued', str(jstats.get('last_queue', 0))))
        print('\t{:>20}: {}'.format('total queuing time', jstats['queue_time'].total_seconds() if jstats.get('queue_time') else 0))
        print('\t{:>20}: {}'.format('first job start', str(jstats.get('first_start', 0))))
        print('\t{:>20}: {}'.format('last job finish', str(jstats.get('last_finish', 0))))
        print('\t{:>20}: {}'.format('total execution time', jstats['execution_time'].total_seconds() if jstats.get('execution_time') else 0))

        print('jobs runtime statistics:')
        for k, v in jstats.get('rstats', {}).items():
            print('\t{:>20}: {}'.format(k, v))

        print('jobs launching statistics:')
        for k, v in jstats.get('launchstats', {}).items():
            print('\t{:>20}: {}'.format(k, v))

    res = stats.resources()
    if res:
        print('available resources:')
        for k, v in res.items():
            print('\t{:>20}: {}'.format(k, v))


def print_text(stats):
    jstats = stats.job_stats()

    if jstats:
        for jname, job in jstats.get('jobs', {}).items():
            jstate = job.get('state')
            jmessages = job.get('messages')
            jhistory = job.get('history')
            jruntime = job.get('runtime')

            print("{} ({}) {}\n\t{}\n\t{}\n".format(
                jname, jstate, jmessages or '',
                "\n\t".join(["{}: {}".format(en.get("date"), en.get("state")) for en in jhistory]),
                "\n\t".join(["{}: {}".format(k, v) for k, v in jruntime.items()])))


@click.group()
def reports():
    pass


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--verbose', is_flag=True, default=False)
def stats(wdir, verbose):
    jobs_report_path = find_report_files(wdir)
    if verbose:
        print(f'found report files: {jobs_report_path} ...')
    log_files = find_log_files(wdir)
    if verbose:
        print(f'found log files: {log_files} ...')

    stats = JobsReportStats(jobs_report_path, log_files, verbose)
    print_stats(stats)


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--verbose', is_flag=True, default=False)
def text(wdir, verbose):
    jobs_report_path = find_report_files(wdir)

    if verbose:
        print(f'found report files: {jobs_report_path} ...')

    stats = JobsReportStats(jobs_report_path, log_files=None, verbose=verbose)
    print_text(stats)


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--details', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def launch_stats(wdir, details, verbose):
    jobs_report_path = find_report_files(wdir)
    if verbose:
        print(f'found report files: {jobs_report_path} ...')
    log_files = find_log_files(wdir)
    if verbose:
        print(f'found log files: {log_files} ...')
    rt_files = find_rtimes_files(wdir)
    if verbose:
        print(f'found rt log files: {rt_files} ...')

    if not rt_files:
        sys.stderr.write(f'error: real time log files not found - cannot generate statistics')
        sys.exit(1)

    stats = JobsReportStats(jobs_report_path, log_files=log_files, rt_files=rt_files, verbose=verbose)
    report = stats.job_start_finish_launch_overheads(details=details)
    print('\t{:>40}: {:.4f}'.format('total start overhead', report.get('start', 0)))
    print('\t{:>40}: {:.4f}'.format('total finish overhead', report.get('finish', 0)))
    print('\t{:>40}: {:.4f}'.format('total start and finish overhead', report.get('total', 0)))
    print('\t{:>40}: {:.4f}'.format('average job start overhead', report.get('job_start_avg', 0)))
    print('\t{:>40}: {:.4f}'.format('average job finish overhead', report.get('job_finish_avg', 0)))
    print('\t{:>40}: {:.4f}'.format('average job total overhead', report.get('job_avg', 0)))
    print('\t{:>40}: {:.4f}'.format('average real job run time', report.get('job_real_rt_avg', 0)))
    print('\t{:>40}: {:.4f}'.format('average qcg job run time', report.get('job_qcg_rt_avg', 0)))
    print('\t{:>40}: {:.2f}'.format('average job overhead per runtime (%)', report.get('job_avg_per_rt', 0)))
    print('\t{:>40}: {}'.format('generated for total jobs', report.get('analyzed_jobs', 0)))
    if details and 'jobs' in report:
        print('\t{:>40}'.format('individual job statistics'))
        for job_name, job_overheads in report.get('jobs', {}).items():
            print(f'\t\t{job_name}: total {job_overheads.get("start", 0) + job_overheads.get("finish", 0):.4f}, '\
                  f'start {job_overheads.get("start"):.4f}, stop {job_overheads.get("finish"):.4f}')


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--details', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def rusage(wdir, details, verbose):
    jobs_report_path = find_report_files(wdir)
    if verbose:
        print(f'found report files: {jobs_report_path} ...')
    log_files = find_log_files(wdir)
    if verbose:
        print(f'found log files: {log_files} ...')
    rt_files = find_rtimes_files(wdir)
    if verbose:
        print(f'found rt log files: {rt_files} ...')

    if not rt_files:
        sys.stderr.write(f'error: real time log files not found - cannot generate statistics')
        sys.exit(1)

    stats = JobsReportStats(jobs_report_path, log_files=log_files, rt_files=rt_files, verbose=verbose)
    report = stats.resource_usage(details=details)

    print('\t{:>40}: {}'.format('used cores', report.get('total_cores', 0)))
    print('\t{:>40}: {:.1f}%'.format('average core utilization (%)', report.get('avg_core_utilization', 0)))

    if details:
        if report.get('nodes'):
         sorted_node_list = sorted(report.get('nodes', {}).keys())
         for node_name in sorted_node_list:
             cores = report['nodes'][node_name]
             print(f'\t{node_name}')
             sorted_core_list = sorted([int(core_id) for core_id in cores.keys()])
             for core_id in sorted_core_list:
                 core_name = str(core_id)
                 core_spec = cores[core_name]
                 print(f'\t\t{core_name:4}: {core_spec.get("utilization", 0):.1f}%, unused {core_spec.get("unused", 0):.4f} s')

  

@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('output', type=click.Path(file_okay=True, dir_okay=False))
@click.option('--real', is_flag=True, default=True)
@click.option('--verbose', is_flag=True, default=False)
def gantt(wdir, output, real, verbose):
    jobs_report_path = find_report_files(wdir)
    if verbose:
        print(f'found report files: {jobs_report_path} ...')
    log_files = find_log_files(wdir)
    if verbose:
        print(f'found log files: {log_files} ...')
    rt_files = find_rtimes_files(wdir)
    if verbose:
        print(f'found log files: {log_files} ...')

    if not rt_files:
        real=False
        print(f'warning: real time log files not found - the generated chart might be not accurate')

    stats = JobsReportStats(jobs_report_path, log_files=log_files, rt_files=rt_files, verbose=verbose)
    try:
        stats.gantt(output, real)
    except ImportError as exc:
        sys.stderr.write(str(exc) + '\n')
        sys.exit(1)


if __name__ == '__main__':
    reports()
