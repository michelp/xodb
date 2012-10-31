import signal
import traceback
import logging


def _usr1_handler(signum, frame):
    logging.debug(''.join(traceback.format_list(traceback.extract_stack())))


def register_signals():
    signal.signal(signal.SIGUSR1, _usr1_handler)

    
