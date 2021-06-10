import os
import logging
import json
import time
import click
import mapreduce.utils
import glob
import threading
import socket
from pathlib import Path


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    def __init__(self, port):
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        #set master to active
        self.active = True

        #initialize port
        self.port = port

        #create new directory and delete old files
        self.temp_path = Path('tmp')
        self.temp_path.mkdir(exist_ok=True)
        for i in glob.glob('tmp/job-*'):
            os.remove(i)

        #initialize containers
        self.threads = []
        self.workers = {}

        #Start job id at 0
        self.jobid = 0

        #create UDP listener and fault tolerance thread
        UDP_listener = threading.Thread(target=self.listen_UDP)
        TCP_listener = threading.Thread(target=self.listen_TCP)

        self.threads.append(UDP_listener)
        self.threads.append(TCP_listener)

        #the threads are run
        for thread in self.threads:
            thread.start()


    def listen_UDP(self):
        #the socket that takes in worker pings
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port - 1))

        sock.settimeout(1)

        #while master is running it waits on pings
        while self.active:
            time.sleep(0.1)
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue

            message_str = message_bytes.decode('utf-8')

            try:
                message_dict = json.loads(message_str)
            except JSONDecodeError:
                continue


    def listen_TCP(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port))
        sock.listen()

        #while master is running it waits on messages
        while self.active:
            time.sleep(0.1)

            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue

            message_chunks = []
            while True:
                try:
                    data = clientsocket.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                message_chunks.append(data)

            clientsocket.close()

            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode('utf-8')

            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue

            self.handle_message(message_dict)
            sock.close()

    def handle_message(self, message):
        msg_type = message['message_type']

        if msg_type == 'shutdown':
            self.shutdown()

        if msg_type == 'register':
            self.register(message)

        if msg_type == 'new_master_job':
            self.new_master_job(message)

    def new_master_job(self, message):
        i_path = message["input_directory"]
        o_path = message["output_directory"]
        m_path = message["mapper_executable"]
        r_path = message["reducer_executable"]
        num_map = message["num_mappers"]
        num_red = message["num_reducers"]

        #assign job id and increment counter
        idstr = 'job-' + str(self.jobid)

        #create new directory for job
        (self.temp_path / idstr).mkdir(exist_ok=True)
        (self.temp_path / idstr / 'mapper-output').mkdir(exist_ok=True)
        (self.temp_path / idstr / 'grouper-output').mkdir(exist_ok=True)
        (self.temp_path / idstr / 'reducer-output').mkdir(exist_ok=True)



    def register(self, message):
        w_host = message['worker_host']
        w_port = message['worker_port']
        w_pid = message['worker_pid']

        worker = {
            'worker_host': w_host,
            'worker_port': w_port,
            'worker_pid': w_pid,
            'status': 'ready'
        }

        self.workers[w_pid] = worker

        worker_msg = {
            'message_type': 'register_ack',
            'worker_host': w_host,
            'worker_port': w_port,
            'worker_pid': w_pid
        }

        self.send_message(w_port, worker_msg)


    def shutdown(self):
        message = {'message_type': 'shutdown'}

        for worker in self.workers.values():
            if worker['status'] != 'dead':
                self.send_message(worker['worker_port'], message)

        self.active = False


    def send_message(self,port,message):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', port))
            json_msg = json.dumps(message)
            sock.sendall(json_msg.encode('utf-8'))
            sock.close()
        except:
            print("Error")


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()
