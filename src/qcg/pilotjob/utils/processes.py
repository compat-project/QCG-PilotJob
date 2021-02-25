import asyncio
import logging
import psutil
import signal
from datetime import datetime


_logger = logging.getLogger(__name__)


async def terminate_subprocess(process, pname, timeout_secs):
    try:
        _logger.info(f'terminating process {pname} ...')
#        process.terminate()
#        process.send_signal(signal.SIGINT)
        process.send_signal(signal.SIGINT)
    except Exception as exc:
        _logger.error(f'failed to cancel process {pname}: {str(exc)}')

    cancel_start_time = datetime.now()

    while True:
        # wait a moment
        _logger.info(f'sleeping waiting for process ...')
        await asyncio.sleep(1)

        # check if processes still exists
        if not process:
            _logger.debug(f'canceled process {pname} finished')
            break
        else:
            pid = process.pid

            try:
                _logger.info(f'checking pid {pid} ...')
                p = psutil.Process(pid)
                _logger.debug(f"process's {pname} pid {pid} still exists")
                # process not finished
            except psutil.NoSuchProcess:
                _logger.debug(f"process's {pname} pid {pid} doesn't exists")
                # process finished
                break
            except Exception as exc:
                _logger.warning(f'failed to check process {pname} status: {str(exc)}')

            if (datetime.now() - cancel_start_time).total_seconds() > timeout_secs:
                # send SIGKILL signal
                try:
                    _logger.info(f'killing processes {pname} pid {pid}')
                    process.kill()
                except Exception as exc:
                    _logger.warning(f'failed to kill process {pname} pid {pid}: {str(exc)}')
                return
