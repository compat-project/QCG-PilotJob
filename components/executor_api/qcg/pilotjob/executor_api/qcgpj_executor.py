import logging
from concurrent.futures import Executor
from enum import Enum

from qcg.pilotjob.api.manager import LocalManager

_logger = logging.getLogger(__name__)


class QCGPJExecutor(Executor):

    def __init__(self,
                 wd=".",
                 resources=None,
                 reserve_core=False,
                 enable_rt_stats=False,
                 wrapper_rt_stats=None,
                 log_level='info',
                 *other_args
                 ):
        """Initialises QCG-PilotJob Executor interface configures a new QCG-PilotJob Manager.

        Parameters
        ----------
        wd : str, optional
            Working directory where QCG-PilotJob manager should be started, by default it is
            a current directory
        resources : str, optional
            The resources to use. If specified forces usage of Local mode of QCG-PilotJob Manager.
            The format is compliant with the NODES format of QCG-PilotJob, i.e.:
            [node_name:]cores_on_node[,node_name2:cores_on_node][,...].
            Eg. to run on 4 cores regardless the node use `resources="4"`
            to run on 2 cores of node_1 and on 3 cores of node_2 use `resources="node_1:2,node_2:3"`
        reserve_core : bool, optional
            If True reserves a core for QCG-PilotJob Manager instance,
            by default QCG-PilotJob Manager shares a core with computing tasks
            Parameters.
        enable_rt_stats : bool, optional
            If True, QCG-PilotJob Manager will collect its runtime statistics
        wrapper_rt_stats : str, optional
            The path to the QCG-PilotJob Manager tasks wrapper program used for collection of statistics
        log_level : str, optional
            Logging level for QCG-PilotJob Manager (for both service and client part).
        other_args : optional
            Optional list of additional arguments for initialisation of QCG-PilotJob Manager

        Returns
        -------
        None

        """

        # ---- QCG PILOT JOB INITIALISATION ---

        # Establish logging levels
        service_log_level, client_log_level = self._setup_qcgpj_logging(log_level)

        # Prepare input arguments for QCG-PJM

        args = ['--log', service_log_level,
                '--wd', wd]

        if resources:
            args.append('--nodes')
            args.append(str(resources))

        if reserve_core:
            args.append('--system-core')

        if enable_rt_stats:
            args.append('--enable-rt-stats')

        if wrapper_rt_stats:
            args.append('--wrapper-rt-stats')
            args.append(wrapper_rt_stats)

        if other_args:
            args.append(other_args)

        client_conf = {'log_file': wd + '/api.log', 'log_level': client_log_level}

        _logger.info(f'Starting QCG-PJ Manager with arguments: {args}')

        # create QCGPJ Manager (service part)
        self._qcgpjm = LocalManager(args, client_conf)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._qcgpjm.finish()

    @property
    def qcgpj_manager(self):
        return self._qcgpjm

    def _setup_qcgpj_logging(self, log_level):
        log_level = log_level.upper()

        try:
            service_log_level = ServiceLogLevel[log_level].value
        except KeyError:
            service_log_level = ServiceLogLevel.DEBUG.value

        try:
            client_log_level = ClientLogLevel[log_level].value
        except KeyError:
            client_log_level = ClientLogLevel.DEBUG.value

        return service_log_level, client_log_level


class ServiceLogLevel(Enum):
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"


class ClientLogLevel(Enum):
    INFO = "info"
    DEBUG = "debug"
