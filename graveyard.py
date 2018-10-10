#!/usr/bin/env python
''' A log aggregator, parser and forwarder'''

import os
import sys
import asyncore
import socket
import logging
import json
from time import sleep
from flask import Flask
from Queue import Queue
from threading import Thread
from numbers import Number
from collections import Set, Mapping, deque
import ssl

# We need to measure our python object sizes
try: # Python 2
    zero_depth_bases = (basestring, Number, xrange, bytearray)
    iteritems = 'iteritems'
except NameError: # Python 3
    zero_depth_bases = (str, bytes, Number, range, bytearray)
    iteritems = 'items'


# Start Flask
api = Flask(__name__)

#Global objects
global log_queue
global log_object
global connection_pool
global client_logs_pool
log_queue = deque()
log_object = {}
connection_pool = {}
client_logs_pool = {}

# Logging config
logging.basicConfig(level=logging.INFO, format='%(name)s:[%(levelname)s]: %(message)s')


# Core config
core_config = {'pause': False,
               'pause_timer': 10,
               'terminate': False,
               'end_test': False
               }

class Worker(Thread):
    """ The thread pool worker """
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as error:
                logging.info(error)
            finally:
                self.tasks.task_done()


class ThreadPool(object):
    """ Here is the pool of threads that will execute our worker """
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """ Add a task to the pool """
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """ Wait for the threads in pool to finish """
        self.tasks.join()


class Server(asyncore.dispatcher):
    def __init__(self, address):
        asyncore.dispatcher.__init__(self)
        self.logger = logging.getLogger('Server')
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(address)
        self.address = self.socket.getsockname()
        self.logger.debug('binding to %s', self.address)
        self.listen(5)

    def handle_accept(self):
        # Called when a client connects to our socket
        client_info = self.accept()
        if client_info is not None:
            self.logger.debug('handle_accept() -> %s', client_info[1])
            ClientHandler(client_info[0], client_info[1])
    

class ClientHandler(asyncore.dispatcher):
    def __init__(self, sock, address):
        asyncore.dispatcher.__init__(self, sock)
        self.logger = logging.getLogger('Client ' + str(address))
        self.data_to_write = []

    def writable(self):
        return bool(self.data_to_write)

    def handle_write(self):
        data = self.data_to_write.pop()
        sent = self.send(data[:1024])
        if sent < len(data):
            remaining = data[sent:]
            self.data.to_write.append(remaining)
        self.logger.debug('handle_write() -> (%d) "%s"', sent, data[:sent].rstrip())

    def handle_read(self):
        data = ''
        while data[-1:].decode("utf-8") != '\n':
            data += self.recv(1)
        for i in data.split("\n"):
            if i not in ('', '\n'):
                try:
                    log_json = json.loads(i, strict=False)
                    log_queue.append(log_json)
                except Exception as err:
                    logging.debug(err)
        #if data[-1:].decode("utf-8") == '\n':
        #    self.logger.debug('handle_read() -> (%d) "%s"', len(data), data)
        

    def handle_close(self):
        self.logger.debug('handle_close()')
        self.close()

def set_keepalive_linux(sock, after_idle_sec=300, interval_sec=120, max_fails=10):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)
    return

def getsize(in_object):
    return sys.getsizeof(in_object)

def bytes_to_mbytes(in_bytes):
    return ('{:,.0f}'.format(in_bytes/float(1<<20)))

def create_socket(host, port, protocol):
    logging.info("New connection to: {0} {1} in {2} mode.".format(host, port, protocol))
    if protocol == "udp":
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #s.setblocking(0)
    set_keepalive_linux(s)
    s.connect((host, port))
    if protocol == "tls":
        return ssl.wrap_socket(s, None, None)
    return s


def send_log(log_object):
    global connection_pool
    sslopt = False
    host_port_protocol = log_object["metadata"].replace(" ", ":")
    host, port, protocol = host_port_protocol.split(":")
    try:
        if not connection_pool.get(host_port_protocol):
            connection_pool[host_port_protocol] = create_socket(host, int(port), protocol)
    except Exception as err:
        logging.info(err)

    try:
        if protocol == "udp":
            connection_pool[host_port_protocol].sendto(log_object["log"] + '\r\n', (host, int(port)))
            return
        connection_pool[host_port_protocol].sendall(log_object["log"] + '\r\n')
    except Exception as err:
        logging.info(err)
    return


def run_send_loop():
    """ Loop to send the logs to the clients """
    global log_queue
    global log_object
    while True:
        try:
            if len(log_queue) > 0:
                send_log(log_queue.pop())
        except Exception as err:
            logging.info(err)
            
    pass

def run_loop(host, port):
    tcp_server = Server((host, port))
    return asyncore.loop()


def api_pause():
    """ Api method for a pause in the scrapping """
    core_config["pause"] = True
    return 'Ok'


def api_start():
    """ Api method to continue after a pause """
    core_config["pause"] = False
    return 'Ok'


def api_queuelist():
    """ Api Method that return the number of remaining itens in queue """
    return str(len(log_queue)) + '\n'

@api.route('/api/v1/<api_module>', methods=['GET', 'POST'])
def main_api(api_module):
    """ Main API function, it'll try to execute the function that match
    with the api_module flask rest method"""
    try:
        return getattr(sys.modules[__name__], "api_{0}".format(api_module))()
    except Exception as error:
        return 'Api module not found or not implemented!\n{0}\n'.format(error)


def main():
    pool = ThreadPool(32)
    pool.add_task(run_loop, "0.0.0.0", 5514)
    pool.add_task(api.run, host='0.0.0.0', port=5000)
    for i in range(1, 30):
        logging.info("Sending worker {0} to the pool".format(i))
        pool.add_task(run_send_loop)
    while core_config["terminate"] is False:
        try:
            if core_config["pause"]:
                sleep(float(core_config["pause_timer"]))
                continue
        except KeyboardInterrupt:
            logging.info("\nWaiting for the thread pool to exit...")
            del pool
            logging.info("\nExiting with {0} itens in queue.".format(len(log_queue)))
            break
        except Exception as err:
            logging.info(err)
        sleep(1)
    return 0

if __name__ == "__main__":
    sys.exit(main())
