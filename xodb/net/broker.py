import zmq
import time
import logging
from cPickle import loads, dumps

from collections import OrderedDict


class RetryError(Exception):
    """A client request was tried too many times. """


class Broker(object):

    def __init__(self, worker_url, client_url, events_url, retry_limit=3):
        self.worker_url = worker_url
        self.client_url = client_url
        self.events_url = events_url
        self.retry_limit = retry_limit

        # the lru of available workers
        self.workers = OrderedDict()

        # busy workers
        self.busy = {}

        # requests whose workers died and need to be retried
        self.retry_list = []

        # Prepare our context and sockets
        self.context = zmq.Context(2)

        self.backend = self.context.socket(zmq.XREP)
        self.backend.bind(self.worker_url)

        self.frontend = self.context.socket(zmq.XREP)
        self.frontend.bind(self.client_url)

        self.events = self.context.socket(zmq.SUB)
        self.events.setsockopt(zmq.SUBSCRIBE, '')
        self.events.connect(self.events_url)

        self.poller = zmq.Poller()

    def run(self):
        """Run forever, polling the sockets. """
        # setup the poller
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.frontend, zmq.POLLIN)
        self.poller.register(self.events, zmq.POLLIN)

        try:
            while True:
                try:
                    socks = dict(self.poller.poll())

                    # any supervisor events?
                    if socks.get(self.events) == zmq.POLLIN:
                        self.handle_event()

                    # any workers joining or replying?
                    if socks.get(self.backend) == zmq.POLLIN:
                        self.handle_backend()

                    # only do something if there is an available
                    # worker to handle it
                    if len(self.workers) > 0:
                        # first see if there are requests that need
                        # retrying because their worker died
                        if self.retry_list:
                            self.handle_retry(self.retry_list.pop())

                        # otherwise check for new requests
                        elif socks.get(self.frontend) == zmq.POLLIN:
                            # dispatch a a request to the oldest
                            # available worker
                            self.handle_worker()
                except Exception:
                    logging.exception('Error in reader loop')
        finally:
            # if the process barfed enough, we get here to clean
            # up. the event handler will clean up our workers.
            time.sleep(1)
            self.frontend.close()
            self.backend.close()
            self.context.term()

    def handle_event(self):
        """ Handle a supervisor event. """
        # get the subscribed event
        headers, eventdata, payload = loads(self.events.recv())
        # listen for dead children
        if headers.get('eventname') == 'PROCESS_STATE_EXITED':
            self.handle_process_state_exited(eventdata)
        elif headers.get('eventname') == 'TICK_5':
            self.handle_tick_5(eventdata)
        elif headers.get('eventname') == 'TICK_60':
            self.handle_tick_60(eventdata)
        elif headers.get('eventname') == 'TICK_3600':
            self.handle_tick_3600(eventdata)

    def handle_process_state_exited(self, eventdata):
        pname = eventdata.get('processname')
        if pname:
            if pname in self.workers:
                # an available worker died, get rid of it
                logging.debug('Removing dead available worker %s' % pname)
                self.workers.pop(pname)
            elif pname in self.busy:
                # a busy worker died, schedule its request for retry
                logging.debug('Removing dead busy %s to retry' % pname)
                self.retry_list.append(self.busy.pop(pname))

    def handle_tick_5(self, eventdata):
        return

    def handle_tick_60(self, eventdata):
        return

    def handle_tick_3600(self, eventdata):
        return

    def handle_backend(self):
        # Get a message from a worker
        message = self.backend.recv_multipart()
        worker_addr = message[0]

        assert message[1] == ''
        client_addr = message[2]

        # if it's a new worker telling us ready...
        if client_addr == 'READY':
            worker_name = message[3]
            # push the new worker into the available queue
            # give it avague allotment of requests to handle
            self.workers[worker_name] = worker_addr
            logging.debug("New worker %s connected from %s" % 
                          (worker_name, worker_addr.encode('hex')))
            return

        # otherwise it's a data reply from a worker
        worker_name = message[2]
        client_addr = message[3]

        # mark the replying worker as available
        self.busy.pop(worker_name, None)
        self.workers[worker_name] = worker_addr
        # process the response from the worker
        assert message[4] == ''
        reply = message[5]

        self.frontend.send_multipart([client_addr, '', reply])

    def handle_retry(self, data):
        """Retry a request given to a busy worker that died mid-request. """

        # pop the request for the worker that died
        client_addr, request, tries = data

        # if it happened too many times, send the client an error
        tries += 1
        if tries >= self.retry_limit:
            self.frontend.send_multipart(
                [client_addr, '', 
                 dumps(RetryError('Error after %s tries.' % tries))]
                )
            return

        # pop a new worker, mark it busy
        worker_name, worker_addr = self.workers.popitem(last=False)
        self.busy[worker_name] = client_addr, request, tries

        logging.debug('Retrying request from %s to %s' % 
                      (client_addr.encode('hex'), worker_name))

        # resend the request to a new worker
        self.backend.send_multipart([worker_addr, "", client_addr, request])

    def handle_worker(self):
        return


def run(name, broker_cls):
    import sys
    from ConfigParser import ConfigParser
    if len(sys.argv) < 1:
        print "usage: %s config_file" % sys.argv[0]
        sys.exit(-1)

    config = ConfigParser()
    config.read(sys.argv[1])
    client_url = config.get(name, 'client_url')
    worker_url = config.get(name, 'worker_url')
    events_url = config.get(name, 'events_url')
    log_file = config.get(name, 'log_file')

    logging.basicConfig(filename=log_file, level=logging.DEBUG)

    s = broker_cls(worker_url, client_url, events_url)
    logging.info('Running %s broker on %s' % (repr(broker_cls), client_url))
    s.run()

