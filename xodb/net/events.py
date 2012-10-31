import os
import zmq
import sys
import logging
from cPickle import dumps
from supervisor import childutils


class SupervisorEventListener(object):
    """Broadcast supervisord events down a 0mq publication socket.

    If certain watched process names die, restart everything, which
    might be too bold.
    """

    def __init__(self, publish_url, watching):
        self.publish_url = publish_url
        self.watching = watching
        self.context = zmq.Context(1)
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(self.publish_url)

    def run(self):
        try:
            while True:
                # get the supervisor event
                headers, payload = childutils.listener.wait(sys.stdin, sys.stdout)
                pheaders, pdata = childutils.eventdata(payload+'\n')
                logging.debug((repr(headers), repr(pheaders), repr(pdata)))

                # # did a broker die?
                # if headers.get('eventname') == 'PROCESS_STATE_EXITED':
                #     if pheaders.get('processname') in self.watching:
                #         # the broker died.  tell supervisor to restart
                #         logging.debug('Restarting supervisord')
                #         i = childutils.getRPCInterface(os.environ)
                #         i.supervisor.restart()
                #         # in theory we never get here...

                # publish the event
                self.socket.send(dumps((headers, pheaders, pdata)))

                # ack to supervisor that we did something
                childutils.listener.ok(sys.stdout)
        except:
            logging.exception('Event listener blew up')
        finally:
            self.socket.close()
            self.context.term()


if __name__ == '__main__':
    from ConfigParser import ConfigParser

    if len(sys.argv) < 1:
        print "usage: %s config_file" % sys.argv[0]
        sys.exit(-1)

    config = ConfigParser()
    config.read(sys.argv[1])

    publish_url = config.get('events', 'publish_url')
    watching = set([name.strip() for name in config.get('events', 'watching').split(',')])
    log_file = config.get('events', 'log_file')

    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    e = SupervisorEventListener(publish_url, watching)
    e.run()
