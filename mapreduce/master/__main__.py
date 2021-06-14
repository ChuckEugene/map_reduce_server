"""Master."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import glob
import threading
import shutil
import socket
from pathlib import Path
import heapq
from contextlib import ExitStack


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    """Master class."""
    
    def __init__(self, port):
        """Initialize master."""
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
            shutil.rmtree(i)

        #initialize containers
        self.threads = []
        self.workers = {}
        self.jobs = []
        self.ping_ids = []

        #Start job id at 0
        self.jobid = 0
        
        self.activejob = False
        self.currentTask = []
        self.sortedNum = 1
        self.currentWorkers = 0
        self.worker_reg_order = []

        #create UDP listener and fault tolerance thread
        UDP_listener = threading.Thread(target=self.listen_UDP)
        TCP_listener = threading.Thread(target=self.listen_TCP)
        Ping_listener = threading.Thread(target=self.fault_tolerance)
        
        
        self.threads.append(UDP_listener)
        self.threads.append(TCP_listener)
        self.threads.append(Ping_listener)

        #the threads are run
        for thread in self.threads:
            thread.start()
        
        TCP_listener.join()
                
        
    def listen_UDP(self):
        """Listen to the UDP."""
        #the socket that takes in worker pings
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
            self.ping_ids.append(message_dict['worker_pid'])

        sock.close()

    def fault_tolerance(self):
        """Use for workers who die."""
        #container tracks workers that missed pings
        missed_pings = {}
        
        #while loop runs every 2 seconds
        while self.active:
            time.sleep(2)

            #any new worker keys are updated
            worker_pids = self.workers.keys()

            #iterate through all workers
            for w_pid in worker_pids:
                
                #if master recieved a ping set its missed pings to zero
                if int(w_pid) in self.ping_ids:
                    missed_pings[w_pid] = 0

                #if not add one
                elif w_pid in missed_pings.keys():
                    missed_pings[w_pid] += 1

                else:
                    missed_pings[w_pid] = 1

                #if 5 missed pings set it to dead
                if missed_pings[w_pid] > 4:
                    if self.workers[w_pid]['status'] != 'dead':
                        self.workers[w_pid]['status'] = 'dead'
                        self.worker_reg_order.remove(w_pid)
                        
                        if self.workers[w_pid]['taskMessage'] != None:
                            workerReady = False
                            for pid in self.worker_reg_order:
                                worker = self.workers[pid]
                                jobDict = self.jobs[0]
                                if worker['status'] == "ready":
                                    
                                    task = self.workers[w_pid]['taskMessage']
                                    task['worker_pid'] = pid
                                    self.workers[worker['worker_pid']]['task'] = task
                                    
                                    self.send_message(worker['worker_port'], self.workers[w_pid]['taskMessage'])                
                                    
                                    self.workers[worker['worker_pid']]['status'] = 'busy'
                                    workerReady = True
                                    break
                                    
                            if workerReady == False:
                                self.currentTask.append(self.workers[w_pid]['taskMessage']['input_files'])
                                
                            

                self.ping_ids = []

            


    def listen_TCP(self):
        """Listen to the TCP."""
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
        """Handle the messages received."""
        msg_type = message['message_type']
        
        if msg_type == 'shutdown':
            self.shutdown()

        if msg_type == 'register':
            self.register(message)
            self.check_jobs()

        if msg_type == 'new_master_job':
            self.new_master_job(message)
            
        if msg_type == 'status':            
            if self.workers[message['worker_pid']]['status'] != "ready":
                self.currentWorkers = self.currentWorkers - 1
                self.workers[message['worker_pid']]['status'] = "ready"
                self.check_jobs()


    def check_jobs(self):
        """Check if jobs are available."""
        if self.activejob:
            self.run_job(self.jobs[0])
            return
        elif len(self.jobs) == 0 or len(self.workers) == 0:
            return
        else: #we have jobs and workers available and a job isn't currently running
            self.run_job(self.jobs[0])
            return

    def run_job(self, jobDict):
        """Run the job."""
        self.activejob = True
        #Run the job
                
        if len(self.currentTask) == 0 and self.currentWorkers == 0 and jobDict['status'] == "map":
            logging.info("Master:%s end map stage", self.port)
            jobDict['status'] = "Startgroup"
        elif len(self.currentTask) == 0 and self.currentWorkers == 0 and jobDict['status'] == "group":
            self.sortedNum = 1
            logging.info("Master:%s end group stage", self.port)
            jobDict['status'] = "Startreduce"

            grouperOut = os.listdir("tmp/job-"+str(jobDict['job_id'])+"/grouper-output")

            files = [open("tmp/job-"+str(jobDict['job_id'])+"/grouper-output/" +fn) for fn in grouperOut]

            with ExitStack() as stack:
                red_files = [stack.enter_context(open("tmp/job-"+str(jobDict['job_id'])+"/grouper-output/reduce" +"{:02d}".format(i+1), 'w'))  for i in range(jobDict['message']['num_reducers'])]

                prevLine = None

                uniq_keys = -1

                for line in heapq.merge(*files):

                    if line != prevLine:
                        uniq_keys = uniq_keys + 1
                        prevLine = line

                    red_files[uniq_keys % jobDict['message']['num_reducers']].write(line)
                    
        elif len(self.currentTask) == 0 and self.currentWorkers == 0 and jobDict['status'] == "reduce":
            logging.info("Master:%s end reduce stage", self.port)
            jobDict['status'] = "done"

            input_files = os.listdir("tmp/job-"+str(jobDict['job_id'])+"/reducer-output")

            (Path(jobDict['message']['output_directory'])).mkdir(exist_ok=True)

            for i, file in enumerate(input_files):
                shutil.copy("tmp/job-"+str(jobDict['job_id'])+"/reducer-output/" + file, jobDict['message']['output_directory'])

                os.rename(jobDict['message']['output_directory'] + "/" + file , jobDict['message']['output_directory'] + "/outputfile" + "{:02d}".format(i+1)) 

            jobDict['status'] = "done"
            self.jobs.pop(0)
            self.activejob = False
            self.check_jobs()
        
        if jobDict['status'] == "StartMap" or jobDict['status'] == "map":
                        
            if len(self.currentTask) == 0 and jobDict['status'] == "StartMap": #self.currentTask initialized
                logging.info("Master:%s begin map stage", self.port)
                
                input_files = os.listdir(jobDict['message']['input_directory'])
                input_files.sort()
                
                self.currentTask = [[jobDict['message']['input_directory'] + "/" + input_files[z] for z in range(y, len(input_files), jobDict['message']['num_mappers'])] 
                                            for y in range(jobDict['message']['num_mappers'])]
                
                jobDict['status'] = "map"
            
            for pid in self.worker_reg_order:
                worker = self.workers[pid]
                if worker['status'] == "ready":
                    if len(self.currentTask) != 0:
                                                
                        message = {
                          "message_type": "new_worker_task",
                          "input_files": self.currentTask[0],
                          "executable": jobDict['message']['mapper_executable'],
                          "output_directory": "tmp/job-"+str(jobDict['job_id'])+"/mapper-output",
                          "worker_pid": worker['worker_pid']
                        }

                        task = self.currentTask.pop(0)
                        self.currentWorkers = self.currentWorkers + 1
                        
                        self.send_message(worker['worker_port'],message)
                        
                        self.workers[worker['worker_pid']]['taskMessage'] = message

                        self.workers[worker['worker_pid']]['status'] = 'busy'
                                    
            if len(self.currentTask) == 0 and self.currentWorkers == 0: 
                logging.info("Master:%s end map stage", self.port)
                jobDict['status'] = "Startgroup"
            
        elif jobDict['status'] == "Startgroup" or jobDict['status'] == "group":
            
            if jobDict['status'] == "Startgroup":
                logging.info("Master:%s begin group stage", self.port)

                input_files = os.listdir("tmp/job-"+str(jobDict['job_id'])+"/mapper-output")
                input_files.sort()

                count = 0

                #workers available
                for worker in self.workers.values():
                    if worker['status'] == "ready":
                        count = count + 1

                if count > len(input_files):
                    count = len(input_files)

                self.currentTask = [["tmp/job-"+str(jobDict['job_id'])+"/mapper-output" + "/" + input_files[z] for z in range(y, len(input_files), count)] 
                                                for y in range(count)]
                
                jobDict['status'] = "group"
            
            
            for pid in self.worker_reg_order:
                worker = self.workers[pid]
                
                if worker['status'] == "ready":
                    if len(self.currentTask) != 0:
                        
                        sortNum = str(self.sortedNum)
                        
                        if self.sortedNum < 10:
                            sortNum = "0" + sortNum
                        
                        message = {
                          "message_type": "new_sort_task",
                          "input_files": self.currentTask[0],
                          "output_file": "tmp/job-"+str(jobDict['job_id'])+"/grouper-output/sorted" + sortNum,
                          "worker_pid": worker['worker_pid']
                        }
                        
                        self.sortedNum = self.sortedNum + 1

                        task = self.currentTask.pop(0)
                        self.currentWorkers = self.currentWorkers + 1

                        self.send_message(worker['worker_port'],message)
                        
                        self.workers[worker['worker_pid']]['taskMessage'] = message
                        self.workers[worker['worker_pid']]['status'] = 'busy'
                        
            
            if len(self.currentTask) == 0 and self.currentWorkers == 0:
                self.sortedNum = 1
                logging.info("Master:%s end group stage", self.port)
                jobDict['status'] = "Startreduce"
                                
                grouperOut = os.listdir("tmp/job-"+str(jobDict['job_id'])+"/grouper-output")
                                
                files = [open("tmp/job-"+str(jobDict['job_id'])+"/grouper-output/" +fn) for fn in grouperOut]
                                                                
                with ExitStack() as stack:
                    red_files = [stack.enter_context(open("tmp/job-"+str(jobDict['job_id'])+"/grouper-output/reduce" +"{:02d}".format(i+1), 'w'))  for i in range(jobDict['message']['num_reducers'])]
                
                    prevLine = None
                    
                    uniq_keys = -1

                    for line in heapq.merge(*files):
                        
                        if line != prevLine:
                            uniq_keys = uniq_keys + 1
                            prevLine = line
                                                                            
                        red_files[uniq_keys % jobDict['message']['num_reducers']].write(line)
            
                            
        elif jobDict['status'] == "Startreduce" or jobDict['status'] == "reduce":
            if jobDict['status'] == "Startreduce":
                logging.info("Master:%s begin reduce stage", self.port)
                
                input_files = os.listdir("tmp/job-"+str(jobDict['job_id'])+"/grouper-output")
                input_files.sort()
                                
                self.currentTask = [["tmp/job-"+str(jobDict['job_id'])+"/grouper-output/reduce" + "{:02d}".format(z+1) for z in range(jobDict['message']['num_reducers'])]
                                            for y in range(jobDict['message']['num_reducers'])]
                
                jobDict['status'] = "reduce"
            
            for pid in self.worker_reg_order:
                worker = self.workers[pid]
                
                if worker['status'] == "ready":
                    if len(self.currentTask) != 0:
                        message = {
                          "message_type": "new_worker_task",
                          "input_files": self.currentTask[0],
                          "executable": jobDict['message']['reducer_executable'],
                          "output_directory": "tmp/job-"+str(jobDict['job_id'])+"/reducer-output",
                          "worker_pid": worker['worker_pid']
                        }
                        

                        task = self.currentTask.pop(0)
                        self.currentWorkers = self.currentWorkers + 1
                        

                        self.send_message(worker['worker_port'],message)
                        
                        
                        self.workers[worker['worker_pid']]['taskMessage'] = message
                        self.workers[worker['worker_pid']]['status'] = 'busy'
                        
            
            if len(self.currentTask) == 0 and self.currentWorkers == 0:
                logging.info("Master:%s end reduce stage", self.port)
                jobDict['status'] = "done"
                
                input_files = os.listdir("tmp/job-"+str(jobDict['job_id'])+"/reducer-output")
                            
                (Path(jobDict['message']['output_directory'])).mkdir(exist_ok=True)

                for i, file in enumerate(input_files):
                    shutil.copy("tmp/job-"+str(jobDict['job_id'])+"/reducer-output/" + file, jobDict['message']['output_directory'])

                    os.rename(jobDict['message']['output_directory'] + "/" + file , jobDict['message']['output_directory'] + "/outputfile" + "{:02d}".format(i+1)) 
                    
                jobDict['status'] = "done"
                self.jobs.pop(0)
                self.activejob = False
                
            
            
        

    def new_master_job(self, message):
        """Set up a new master job."""
        #assign job id and increment counter
        idstr = 'job-' + str(self.jobid)

        #create new directory for job
        (self.temp_path / idstr).mkdir(exist_ok=True)
        (self.temp_path / idstr / 'mapper-output').mkdir(exist_ok=True)
        (self.temp_path / idstr / 'grouper-output').mkdir(exist_ok=True)
        (self.temp_path / idstr / 'reducer-output').mkdir(exist_ok=True)

        self.jobs.append({"message": message, "job_id": self.jobid, "status": "StartMap"})
        #Status: "map" -> "group" -> "reduce"
        self.jobid += 1
        self.check_jobs()



    def register(self, message):
        """Register workers."""
        w_host = message['worker_host']
        w_port = message['worker_port']
        w_pid = message['worker_pid']

        worker = {
            'worker_host': w_host,
            'worker_port': w_port,
            'worker_pid': w_pid,
            'status': 'ready',
            'taskMessage': None
        }

        self.workers[w_pid] = worker
        
        self.worker_reg_order.append(w_pid)

        worker_msg = {
            'message_type': 'register_ack',
            'worker_host': w_host,
            'worker_port': w_port,
            'worker_pid': w_pid
        }
        
        self.send_message(w_port, worker_msg)



    def shutdown(self):
        """Shuts down master and workers."""
        message = {'message_type': 'shutdown'}

        for worker in self.workers.values():
            if worker['status'] != 'dead':
                self.send_message(worker['worker_port'], message)

        self.active = False




    def send_message(self,port,message):
        """Send message to worker."""
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
    """First function."""
    Master(port)


if __name__ == '__main__':
    main()
