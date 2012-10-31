import logging
from cPickle import dumps, loads

from xodb.net.worker import Worker, run


class Reader(Worker):

    def handle_request(self):
        client, data = self.socket.recv_multipart()
        try:
            m, args, kwargs = loads(data)
            result = getattr(self, 'handle_' + m)(*args, **kwargs)
            self.socket.send_multipart([self.name, client, '', dumps(result)])
        except Exception, e:
            logging.exception('Error handling request.')
            self.socket.send_multipart([self.name, client, '', dumps(e)])

    def handle_count(self, *args, **kwargs):
        return self.db.count(*args, **kwargs)

    def handle_allterms(self, *args, **kwargs):
        return list(self.db.allterms(*args, **kwargs))

    def handle_query(self, *args, **kwargs):
        if 'limit' not in kwargs:
            raise TypeError('Remote queries require a limit')
        return list(self.db.query(*args, **kwargs))

if __name__ == '__main__':
    run('reader', Reader)
