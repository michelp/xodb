import time
import zmq

from xodb.tools import lazy_property
from cPickle import dumps, loads


class TimeoutError(Exception):
    pass


class Method(object):
    """A methodish handle on an rpc call to a remote method.  When
    called, synchronously sends the request and waits for a
    response.

    The objects may timeout depending on the client object that
    creates them.
    """

    def __init__(self, name, client, retry_limit=3, timeout=5000):
        self.name = name
        self.client = client

    @lazy_property
    def socket(self):
        socket = self.client.context.socket(zmq.REQ)
        self.client.poller.register(socket, zmq.POLLIN)
        socket.connect(self.client.client_url)
        return socket

    def __call__(self, *args, **kwargs):
        """Send the request, poll for a reasonable time. If there is
         no reply, close and reconnect the client then do it all over
         again a reasonable amount of times.  If all that fails, die.

        This loosly based on the 'Lazy Pirate Pattern' in the 0mq
        guide.
        """
        self.socket.send(dumps((self.name, args, kwargs)))
        tries = 0
        try:
            while tries < self.client.retry_limit:
                socks = dict(self.client.poller.poll(self.client.timeout))
                if socks.get(self.socket) == zmq.POLLIN:
                    return loads(self.socket.recv())
                tries += 1
                time.sleep(.01 * tries)
            else:
               raise TimeoutError('Timeout for %s' % repr((self.name, args, kwargs)))
        finally:
            self.socket.close()



class Promise(Method):
    """Methodish object that when called, sends an RPC request and
    then returns immediately, not waiting for an answer.  Later, the
    answer can be retreived by the 'value' attribute, which may block
    or timeout.  To check to see if 'value' will block, check the
    'ready' property.
    """

    def __call__(self, *args, **kwargs):
        """Send the request but do not block."""
        self.socket.send(dumps((self.name, args, kwargs)))
        return self

    @property
    def ready(self):
        socks = dict(self.client.poller.poll(0))
        if socks.get(self.socket) == zmq.POLLIN:
            return True
        return False

    @lazy_property
    def value(self):
        """
        Poll for a reasonable time. If there is no reply, close and
         reconnect the client then do it all over again a reasonable
         amount of times.  If all that fails, die.

        This loosly based on the 'Lazy Pirate Pattern' in the 0mq
        guide.

        The socket is closed after calling this method.  The promise
        is either fulfilled, or an error occured.
        """
        tries = 0
        try:
            while tries < self.client.retry_limit:
                socks = dict(self.client.poller.poll(self.client.timeout))
                if socks.get(self.socket) == zmq.POLLIN:
                    return loads(self.socket.recv())
                tries += 1
                time.sleep(.01 * tries)
            else:
                raise TimeoutError('Timeout for %s' % repr(self.name))
        finally:
            self.socket.close()


class Client(object):
    """RPC client to an xodb database.
    """

    def __init__(self, client_url, timeout=10000, retry_limit=3):
        self.client_url = client_url
        self.timeout = timeout
        self.retry_limit = retry_limit
        self.connect()

    def connect(self):
        self.context = zmq.Context(1)
        self.poller = zmq.Poller()

    def close(self):
        self.context.term()

    def __getattr__(self, name):
        return Method(name, self)


class PromiseClient(Client):

    def __getattr__(self, name):
        return Promise(name, self)


if __name__ == "__main__":
    from xodb.tools.signals import register_signals
    register_signals()

    import sys
    from ConfigParser import ConfigParser

    if len(sys.argv) < 1:
        print "usage: %s config_file" % sys.argv[0]
        sys.exit(-1)

    config = ConfigParser()
    config.read(sys.argv[1])

    client_config = dict(config.items('client'))

    client_url = client_config.get('client_url')
    log_file = client_config.get('log_file')
    timeout = int(client_config.get('timeout'))
    retry_limit = int(client_config.get('retry_limit'))

    import random
    c = Client(client_url, timeout, retry_limit)
    p = PromiseClient(client_url, timeout, retry_limit)
