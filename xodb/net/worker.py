import os
import sys
import zmq
import time
import logging
from cPickle import dumps, loads


POLL_TIME = 1000


class Worker(object):

    def __init__(self, name, worker_url, db):
        self.name = name
        self.worker_url = worker_url
        self.db = db
        self.context = zmq.Context(1)
        self.socket = self.context.socket(zmq.REQ)
        self.poller = zmq.Poller()

    def run(self):
        try:
            self.poller.register(self.socket, zmq.POLLIN)
            self.socket.connect(self.worker_url)
            logging.debug('Sending ready message to server.')
            self.socket.send_multipart(['READY', self.name])
            logging.debug('Ready to handle requests.')
            while True:
                try:
                    socks = dict(self.poller.poll(POLL_TIME))
                    if socks.get(self.socket) == zmq.POLLIN:
                        self.handle_request()
                except Exception:
                    logging.exception('Error in read_worker inner loop')
        except Exception:
            logging.exception('Error in read_worker run method')
        finally:
            self.die()

    def handle_request(self):
        return

    def die(self):
        time.sleep(1)
        self.socket.close()
        self.context.term()
        sys.exit()


def run(section, worker_cls):
    import xodb
    from xodb.tools.signals import register_signals
    register_signals()
    name = os.environ.get('SUPERVISOR_PROCESS_NAME', 'tester')
    from ConfigParser import ConfigParser

    if len(sys.argv) < 1:
        print "usage: %s config_file" % sys.argv[0]
        sys.exit(-1)

    config = ConfigParser()
    config.read(sys.argv[1])

    worker_url = config.get(section, 'worker_url')
    log_file = config.get(section, 'log_file')
    db_path = config.get(section, 'db_path')

    logging.basicConfig(filename=log_file % name, level=logging.DEBUG)
    db = xodb.open(db_path, writable=False)
    w = worker_cls(name, worker_url, db)
    logging.debug('Running worker on %s' % worker_url)
    w.run()
