import os
import logging
import json
import time
import click
import mapreduce.utils
import glob
import threading
import socket


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
        Path('tmp').mkdir(exist_ok=True)
        for i in glob.glob('tmp/job-*')
            os.remove(i)

        #initialize thread container
        self.threads = []

        #create UDP listener and fault tolerance thread
        listener = threading.Thread(target=self.listen_for_UDP)
        fault_tol = threading.Thread(target=self.fault_tolerance)

        self.threads.append(listener)
        self.threads.append(fault_tol)

        #the threads are run 
        for thread in threads:
            thread.start()


    def listen_for_UDP(self):
        #the socket that takes in jobs is created
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port - 1))

        sock.settimeout(1)



        try:
            msg = json.loads(msg)
        except JSONDecodeError:
            continue

    def fault_tolerance(self):






@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()
