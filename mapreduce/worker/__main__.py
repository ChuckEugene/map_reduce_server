import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
import subprocess


# Configure logging
logging.basicConfig(level=logging.DEBUG)
    

class Worker:
    def __init__(self, master_port, worker_port):
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())
        
        self.alive = False
        
        # Get PID
        self.pid = os.getpid()
                
        listen_thread = threading.Thread(target=self.listeningToMaster, args=(worker_port,master_port,))
        listen_thread.start()
                
        # Send register message to Master
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", master_port))
        
        register_message = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": worker_port,
            "worker_pid": self.pid
        }
        
        message = json.dumps(register_message)
        sock.sendall(message.encode('utf-8'))
        sock.close()
                
        listen_thread.join() # wait for the listen_thread to return


    def listeningToMaster(self,worker_port, master_port):
        # TCP Socket that listens for instructions

        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the server
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_sock.bind(("localhost", worker_port))
        listen_sock.listen()

        # Socket accept() and recv() will block for a maximum of 1 second.  If you
        # omit this, it blocks indefinitely, waiting for a connection.
        listen_sock.settimeout(1)

        while True:
            # Wait for a connection for 1s.  The socket library avoids consuming
            # CPU while waiting for a connection.
            try:
                clientsocket, address = listen_sock.accept()
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
            except json.JSONDecodeError:
                continue

            if message_dict['message_type'] == "shutdown":
                self.alive = False
                break
            elif message_dict['message_type'] == "register_ack": 
                self.alive = True
                self.heart_thread = threading.Thread(target=self.heartbeat, args=(master_port,))
                self.heart_thread.start()
            elif message_dict['message_type'] == "new_worker_task":
                
                output_files = []
                
                for file in message_dict['input_files']:
                    with open(file) as inFile:
                        with open(message_dict['output_directory'] + "/" + os.path.basename(file) , 'w') as outFile:
                            output_files.append(message_dict['output_directory'] + "/" + os.path.basename(file))
                            subprocess.run([message_dict['executable']], stdin=inFile, stdout=outFile)
                
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(("localhost", master_port))

                register_message = {
                  "message_type": "status",
                  "output_files" : output_files,
                  "status": "finished",
                  "worker_pid": self.pid
                }

                message = json.dumps(register_message)
                sock.sendall(message.encode('utf-8'))
                sock.close()
            elif message_dict['message_type'] == "new_sort_task":
                
                words = []
                
                for file in message_dict['input_files']:
                    with open(file, 'r') as inFile:
                        for line in inFile.readlines():
                            words.append(line)
                    
                
                words.sort()
                
                with open(message_dict['output_file'], 'w') as outFile:
                    for word in words:
                        outFile.write(word)
                
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(("localhost", master_port))

                register_message = {
                  "message_type": "status",
                  "output_file" : message_dict['output_file'],
                  "status": "finished",
                  "worker_pid": self.pid
                }

                message = json.dumps(register_message)
                sock.sendall(message.encode('utf-8'))
                sock.close()
    
    def heartbeat(self,master_port):
                
        while self.alive == True:
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
        


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()
