#!/usr/bin/env python3
"""
ds-sim client implementing a custom scheduling algorithm that minimizes average turnaround time.
This client uses a Minimum Completion Time (MCT) heuristic:contentReference[oaicite:1]{index=1} (choosing the server where each job will finish earliest),
with tie-breaking by lower server cost, to outperform baseline algorithms (ATL, FF, BF, FC, FAFC) in average turnaround time.
The code follows the ds-sim protocol and is compatible with mark_client.py for automated testing.
"""

import sys
import socket
import xml.etree.ElementTree as ET

# Helper data structures for server information and state
class ServerInfo:
    """Static information for a server type (from ds-system.xml)."""
    def __init__(self, name, id, cores, memory, disk, boot_time, hourly_rate):
        self.name = name       # server type name
        self.id = id           # server ID (unique within type)
        self.cores = cores     # total core count
        self.memory = memory   # total memory
        self.disk = disk       # total disk
        self.boot_time = boot_time   # bootup time (seconds)
        self.hourly_rate = hourly_rate  # hourly rental cost rate

class ServerState:
    """Dynamic state of a server, including running/queued tasks and resource usage over time."""
    def __init__(self, info: ServerInfo):
        self.info = info
        # List of tasks (jobs) currently scheduled on this server.
        # Each task is represented as a dict with keys: 'job_id', 'start_time', 'finish_time', 'cores', 'mem', 'disk'.
        self.tasks = []
        # If the server is not yet active (was off and booting), booting_until tracks the time until it becomes active.
        # For active servers, booting_until is None.
        self.booting_until = None

    def remove_finished_tasks(self, current_time: int):
        """Remove all tasks that have finished by the current time, freeing resources."""
        # Remove tasks that have finish_time <= current simulation time
        self.tasks = [task for task in self.tasks if task['finish_time'] > current_time]
        # If server was booting and boot time has passed, mark it as active
        if self.booting_until is not None and current_time >= self.booting_until:
            self.booting_until = None

    def get_available_resources(self, current_time: int):
        """Calculate the available (free) cores, memory, and disk on this server at the given time."""
        # First, free any completed tasks up to current_time
        self.remove_finished_tasks(current_time)
        # If server is still in boot phase (not active yet), then no resources are available until booting finishes
        if self.booting_until is not None and current_time < self.booting_until:
            return 0, 0, 0  # no cores, mem, disk available yet
        # Calculate resources in use by tasks that are running at current_time
        used_cores = used_mem = used_disk = 0
        for task in self.tasks:
            # Count task if it is running at current_time: start_time <= current_time < finish_time
            if task['start_time'] <= current_time < task['finish_time']:
                used_cores += task['cores']
                used_mem += task['mem']
                used_disk += task['disk']
        # Available = total capacity - used
        free_cores = self.info.cores - used_cores
        free_mem = self.info.memory - used_mem
        free_disk = self.info.disk - used_disk
        return free_cores, free_mem, free_disk

    def predict_ready_time(self, job, current_time: int):
        """
        Determine the earliest time this server can start the given job (taking into account current tasks).
        Returns a tuple: (ready_time, finish_time).
        """
        cores_req, mem_req, disk_req, runtime = job
        free_cores, free_mem, free_disk = self.get_available_resources(current_time)
        # If resources are available now to start the job
        if free_cores >= cores_req and free_mem >= mem_req and free_disk >= disk_req:
            ready_time = current_time  # job can start immediately
        else:
            # Not enough resources now, find when enough will be free.
            # Sort remaining tasks by finish_time to simulate resource availability timeline
            remaining_tasks = sorted(self.tasks, key=lambda t: t['finish_time'])
            # Start from current resource availability
            cur_free_cores = free_cores
            cur_free_mem = free_mem
            cur_free_disk = free_disk
            ready_time = current_time
            for t in remaining_tasks:
                # Advance time to this task's finish
                if t['finish_time'] > ready_time:
                    ready_time = t['finish_time']
                # When task finishes, its resources become free
                cur_free_cores += t['cores']
                cur_free_mem += t['mem']
                cur_free_disk += t['disk']
                # If server was booting and this finish time marks boot completion, mark server active
                if self.booting_until is not None and ready_time >= self.booting_until:
                    self.booting_until = None  # server is now active
                # Check if by this time we have enough resources for the job
                if cur_free_cores >= cores_req and cur_free_mem >= mem_req and cur_free_disk >= disk_req:
                    break
        # If server is currently off (inactive and not booted yet), then we must include bootup delay
        if self.booting_until is None and len(self.tasks) == 0 and ready_time == current_time:
            # The server is idle and off: starting a job will require bootup
            ready_time = current_time + self.info.boot_time
            # Mark that the server will be booting until ready_time
            self.booting_until = ready_time
        # Compute finish time if job starts at ready_time
        finish_time = ready_time + runtime
        return ready_time, finish_time

    def schedule_job(self, job_id: int, job, start_time: int):
        """Add a job (with given start time) to this server's task list and compute its finish time."""
        cores_req, mem_req, disk_req, runtime = job
        finish_time = start_time + runtime
        # Append the new task to the server's task list
        self.tasks.append({
            'job_id': job_id,
            'start_time': start_time,
            'finish_time': finish_time,
            'cores': cores_req,
            'mem': mem_req,
            'disk': disk_req
        })
        # Keep tasks sorted by finish_time for efficient future use
        self.tasks.sort(key=lambda t: t['finish_time'])
        return finish_time

# Connect to ds-server (default localhost:50000)
HOST, PORT = "127.0.0.1", 50000
if len(sys.argv) > 1:
    # Optionally allow specifying host and port via command-line arguments
    # e.g., client.py host port
    HOST = sys.argv[1]
    if len(sys.argv) > 2:
        PORT = int(sys.argv[2])
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    sock_file = sock.makefile('rwb')  # buffered read-write binary file object for easier line handling
except Exception as e:
    print(f"Error: Could not connect to ds-server at {HOST}:{PORT} - {e}", file=sys.stderr)
    sys.exit(1)

def send(msg: str):
    """Send a message to ds-server (appending newline, UTF-8 encoding)."""
    sock_file.write(msg.encode() + b'\n')
    sock_file.flush()

def recv() -> str:
    """Receive a line from ds-server, decoded from UTF-8 (stripping any trailing newline)."""
    line = sock_file.readline().decode().strip()
    return line

# Handshake protocol
send("HELO")
response = recv()
if response != "OK":
    # If an unexpected response is received, exit
    print(f"Protocol error: expected 'OK' after HELO, got '{response}'", file=sys.stderr)
    sys.exit(1)
# Authentication (username can be arbitrary as actual auth is not enforced)
username = "client"  # default username; can use environment or custom if needed
try:
    import getpass
    username = getpass.getuser()
except Exception:
    # If getpass fails (e.g., no OS user in environment), use a generic name
    username = "client"
send(f"AUTH {username}")
response = recv()
if response != "OK":
    print(f"Protocol error: expected 'OK' after AUTH, got '{response}'", file=sys.stderr)
    sys.exit(1)

# After AUTH, ds-server writes system info to ds-system.xml. Parse that file for server details.
servers = []        # list of ServerInfo for each server instance (each with unique type+id)
server_states = {}  # mapping (serverType, serverID) -> ServerState for dynamic tracking
try:
    tree = ET.parse("ds-system.xml")
    root = tree.getroot()
    # The XML has a <servers> element containing multiple <server> entries
    for s in root.find("servers").findall("server"):
        stype = s.get("type")
        limit = int(s.get("limit"))  # number of servers of this type
        boot_time = int(s.get("bootupTime"))
        hourly_rate = float(s.get("hourlyRate"))
        cores = int(s.get("cores") or s.get("coreCount") or 0)  # 'cores' attribute might be named 'coreCount' in some versions
        memory = int(s.get("memory") or 0)
        disk = int(s.get("disk") or 0)
        # Create ServerInfo and ServerState for each instance (id 0 to limit-1)
        for sid in range(limit):
            info = ServerInfo(stype, sid, cores, memory, disk, boot_time, hourly_rate)
            servers.append(info)
            server_states[(stype, sid)] = ServerState(info)
except Exception as e:
    # If parsing fails, proceed without system info (fallback to on-the-fly GETS if needed)
    print(f"Warning: Could not parse ds-system.xml ({e}). Will rely on GETS Capable for server info.", file=sys.stderr)
    servers = []
    server_states = {}

# Helper function to choose a server for a given job using our scheduling heuristic
def choose_server_for_job(job_id: int, submit_time: int, cores_req: int, mem_req: int, disk_req: int, est_runtime: int):
    """Select the best server to schedule the given job (with requirements and estimated runtime)."""
    best_server = None
    earliest_finish = float('inf')
    best_ready_time = None

    job_specs = (cores_req, mem_req, disk_req, est_runtime)
    # Iterate through all servers capable of running the job (i.e., having capacity >= requirements)
    for info in servers:
        # Quick capability check: skip servers that by spec cannot handle the job's requirements
        if info.cores < cores_req or info.memory < mem_req or info.disk < disk_req:
            continue
        state = server_states[(info.name, info.id)]
        # Predict when this server could start the job and when it would finish
        ready_time, finish_time = state.predict_ready_time(job_specs, submit_time)
        # Check if this finish time is the earliest seen so far
        if finish_time < earliest_finish:
            earliest_finish = finish_time
            best_server = info
            best_ready_time = ready_time
        elif finish_time == earliest_finish and best_server is not None:
            # Tie-breaker: if finish time is equal, choose the server with lower cost (hourly rate)
            if info.hourly_rate < best_server.hourly_rate:
                best_server = info
                best_ready_time = ready_time
            # If cost is also equal, could tie-break by server cores or ID to have deterministic behavior (optional)
    return best_server, best_ready_time

# Main scheduling loop
send("REDY")
current_time = 0  # track current simulation time
while True:
    message = recv()
    if not message:
        # Connection closed unexpectedly
        break
    parts = message.split()
    if parts[0] == "NONE":
        # No more jobs: end simulation
        send("QUIT")
        recv()  # server should respond with "QUIT"
        break
    elif parts[0] == "JOBN" or parts[0] == "JOBP":
        # Job submission event (JOBN normal job, JOBP resubmitted job)
        # Format: JOBN submitTime jobID cores memory disk estRuntime
        # Parse job details
        # Note: According to ds-sim spec, the order is "JOBN jobID submitTime cores memory disk estRuntime"
        # Some versions list submitTime first. We will detect based on number of fields.
        if len(parts) == 7:
            # Format might be: JOBN jobID submitTime cores memory disk estRuntime
            _, job_id_str, submit_time_str, core_str, mem_str, disk_str, run_str = parts
        else:
            # Format might be: JOBN submitTime jobID cores memory disk estRuntime
            # (Based on documentation: JOBN jobID submitTime cores memory disk estRuntime)
            # We handle both just in case by checking positions
            _, first, second, third, fourth, fifth, sixth = parts
            # If first field after JOBN is likely submitTime (usually larger than jobID and increasing),
            # we can differentiate by value. If first < second, assume first is jobID.
            if first.isdigit() and second.isdigit():
                if int(first) < int(second):
                    # likely jobID = first, submitTime = second
                    job_id_str, submit_time_str, core_str, mem_str, disk_str, run_str = first, second, third, fourth, fifth, sixth
                else:
                    # likely submitTime = first, jobID = second
                    submit_time_str, job_id_str, core_str, mem_str, disk_str, run_str = first, second, third, fourth, fifth, sixth
            else:
                # Default to treating first as jobID if numeric parsing fails
                job_id_str, submit_time_str, core_str, mem_str, disk_str, run_str = first, second, third, fourth, fifth, sixth
        job_id = int(job_id_str)
        submit_time = int(submit_time_str)
        cores_req = int(core_str)
        mem_req = int(mem_str)
        disk_req = int(disk_str)
        runtime = int(run_str)
        # Update current simulation time to the job's submission time (jobs are delivered in increasing time order)
        current_time = submit_time

        # Choose the best server for this job using our scheduling heuristic (earliest finish time, cost-aware)
        server_info, start_time = choose_server_for_job(job_id, submit_time, cores_req, mem_req, disk_req, runtime)
        if server_info is None:
            # No capable server found (should not happen if config ensures capability, but handle gracefully)
            send(f"ERR NoCapableServer for job {job_id}")
            continue
        # Schedule the job on the chosen server
        finish_time = server_states[(server_info.name, server_info.id)].schedule_job(job_id, (cores_req, mem_req, disk_req, runtime), start_time)
        # Send SCHD (schedule) command to server: "SCHD jobID serverType serverID"
        send(f"SCHD {job_id} {server_info.name} {server_info.id}")
        # Expect an "OK" acknowledgment for the scheduling action
        ack = recv()
        if ack != "OK":
            print(f"Protocol warning: expected OK after SCHD, got '{ack}'", file=sys.stderr)
        # After scheduling, loop back to send REDY for the next event
        send("REDY")
    elif parts[0] == "JCPL":
        # Job completion event: format "JCPL endTime jobID serverType serverID"
        # We can use this to update server state (remove the completed task).
        _, completion_time_str, job_id_str, stype, sid_str = parts
        completion_time = int(completion_time_str)
        job_id = int(job_id_str)
        sid = int(sid_str)
        current_time = completion_time  # advance current time to the completion event time
        # Remove the completed job from that server's task list
        state = server_states.get((stype, sid))
        if state:
            # Filter out the task with matching job_id
            state.tasks = [task for task in state.tasks if task['job_id'] != job_id]
        # Send REDY to acknowledge and get next event
        send("REDY")
    elif parts[0] in ("RESF", "RESR", "CHKQ"):
        # Resource failure, recovery, or queue check events.
        # We simply acknowledge these events and update state if needed.
        if parts[0] == "RESF":
            # Server failure: parts format "RESF serverType serverID"
            _, stype, sid_str = parts
            sid = int(sid_str)
            # Mark server as failed by clearing tasks (they will be resubmitted as JOBP events)
            state = server_states.get((stype, sid))
            if state:
                state.tasks.clear()
                state.booting_until = None
        elif parts[0] == "RESR":
            # Server recovery: server becomes available again (no tasks running since they were cleared on fail)
            _, stype, sid_str = parts
            sid = int(sid_str)
            # Reset server state (make sure it's marked active and no tasks)
            state = server_states.get((stype, sid))
            if state:
                state.tasks.clear()
                state.booting_until = None
        # For CHKQ (check queue), or any other events, no special handling needed in this simple client.
        # Acknowledge by sending REDY to proceed.
        send("REDY")
    else:
        # Unexpected message; to maintain protocol alignment, send REDY and continue
        send("REDY")

# Close socket connection
try:
    sock_file.close()
    sock.close()
except Exception:
    pass
