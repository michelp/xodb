
XODB client/server code is based on 0mq.

There can be any number of clients.

There is one reader broker and any number of workers, all managed by
supervisor(s).

The read broker routes requests from clients to workers.

If all workers are busy, the clients queue up.

Supervisor events are published on a PUB socket for reader/workers to
subscribe to.

If a worker dies, supervisor restarts it and notified the reader via
an event.

The reader makes sure dead workers are no longer available for
clients.

If a worker dies while handling a client request, the request will be
retried again up to N times.


----------------------------------------------------------------------


        +--------+  +--------+  +--------+  +--------+
        | Client |  | Client |  | Client |  | Client |
        +--------+  +--------+  +--------+  +--------+  N Client
        |  REQ   |  |  REQ   |  |  REQ   |  |  REQ   |  Processes
        +---+----+  +---+----+  +---+----+  +---+----+
            |           |           |           |
            +-----------+-----------+-----------+
                                    |
                                    | (tcp)
                                    |
+-----------------------------------|-------------------+
|                                   |                   |
| +------------+                +---+----+              |
| | Supervisor |                | ROUTER |              |
| +------------+ (ipc) +--------+--------+              |
| | Event PUB  +-------+  SUB   | READER |              |
| +------------+       +--------+--------+              |
|                               | ROUTER |              |
|                               +---+----+              |
|                                   |                   |
|                                   | (ipc)             |
|                                   |                   |
|  Request to LRU       +-----------+-----------+       |
|     Worker            |           |           |       |
|                   +---+----+  +---+----+  +---+----+  |
|                   |  REQ   |  |  REQ   |  |  REQ   |  |
|    N Worker       +--------+  +--------+  +--------+  |
|    Processes      | Worker |  | Worker |  | Worker |  |
|                   +--------+  +--------+  +--------+  |
|                                                       |
+-------------------------------------------------------+
| Databases on disk                                     |
|                                                       |
|      +--------+  +--------+  +--------+  +--------+   |
|      | DB1    |  | DB2    |  | DB3    |  | DB4    |   |
|      +--------+  +--------+  +--------+  +--------+   |
|                                                       |
+-------------------------------------------------------+


The key to a ROUTER is, you tell it where to send something, it tells
you where it got something.

