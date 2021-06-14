"""Worker main module."""

import os
import logging
import json
import time
import threading
import socket
import subprocess
import click

# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    """Worker class."""

    def __init__(self, master_port, worker_port):
        """Init func."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        self.alive = False

        # Get PID
        self.pid = os.getpid()

        listen_thread = threading.Thread(target=self.listen_master,
                                         args=(worker_port, master_port,))
        listen_thread.start()

        self.heart_thread = threading.Thread(target=self.heartbeat,
                                             args=(master_port,))

        # Send register message to Master

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", master_port))

        register_message = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": worker_port,
            "worker_pid": os.getpid()
        }

        message = json.dumps(register_message)
        sock.sendall(message.encode('utf-8'))
        sock.close()

        listen_thread.join()

    def listen_master(self, worker_port, master_port):
        """TCP Socket that listens for instructions."""
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the server
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_sock.bind(("localhost", worker_port))
        listen_sock.listen()

        # Socket accept() and recv() will block for a maximum of 1 second.
        # omit this, it blocks indefinitely, waiting for a connection.
        listen_sock.settimeout(1)

        while self.alive:
            # Wait for a connection for 1s.  The socket library avoids
            # CPU while waiting for a connection.
            try:
                clientsocket, _ = listen_sock.accept()
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

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
                logging.debug(
                    "Master:%s received\n%s",
                    worker_port,
                    json.dumps(message_dict, indent=2),
                )
            except json.JSONDecodeError:
                continue

            if message_dict['message_type'] == "shutdown":
                self.alive = False
            elif message_dict['message_type'] == "register_ack":
                self.alive = True
                self.heart_thread.start()
            elif message_dict['message_type'] == "new_worker_task":
                self.new_task(message_dict, master_port)
            elif message_dict['message_type'] == "new_sort_task":
                self.new_sort(message_dict, master_port)

        listen_sock.close()

    def heartbeat(self, master_port):
        """Heartbeat sent to master."""
        while self.alive:
            # Create an INET, DGRAM socket, this is UDP
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            # Connect to the UDP socket on server
            sock.connect(("localhost", master_port - 1))

            message = {
                "message_type": "heartbeat",
                "worker_pid": self.pid
            }
            # Send a message
            message = json.dumps(message)
            sock.sendall(message.encode('utf-8'))
            sock.close()
            time.sleep(2)

    def new_task(self, message_dict, master_port):
        """Task case for worker."""
        output_files = []

        for file in message_dict['input_files']:
            with open(file) as in_file:
                output_dir = message_dict['output_directory'] + "/"
                output_dir = output_dir + + os.path.basename(file)
                with open(output_dir, 'w') as out_file:
                    output_files.append(output_dir)
                    subprocess.run([message_dict['executable']],
                                   stdin=in_file, stdout=out_file,
                                   check=True)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", master_port))

        register_message = {
          "message_type": "status",
          "output_files": output_files,
          "status": "finished",
          "worker_pid": self.pid
        }

        message = json.dumps(register_message)
        sock.sendall(message.encode('utf-8'))
        sock.close()

    def new_sort(self, message_dict, master_port):
        """Sorting task for worker."""
        words = []

        for file in message_dict['input_files']:
            with open(file, 'r') as in_file:
                for line in in_file.readlines():
                    words.append(line)

        words.sort()

        with open(message_dict['output_file'], 'w') as out_file:
            for word in words:
                out_file.write(word)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", master_port))

        register_message = {
          "message_type": "status",
          "output_file": message_dict['output_file'],
          "status": "finished",
          "worker_pid": self.pid
        }

        message = json.dumps(register_message)
        sock.sendall(message.encode('utf-8'))
        sock.close()


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    """Yo this is the function."""
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()
