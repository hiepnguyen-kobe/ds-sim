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
from collections import defaultdict

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

# Utilities for ds-server query protocol (GETS ... -> DATA n)
def parse_server_record(line: str):
    parts = line.split()
    # Expected format (per ds-client): type id state curStartTime cores mem disk
    # Some versions omit curStartTime; fall back by shifting indices if needed
    if len(parts) >= 7:
        stype = parts[0]
        sid = int(parts[1])
        state = parts[2]
        # Handle both formats: with and without curStartTime
        if parts[3].isdigit() and len(parts) >= 8:
            # With curStartTime, resources start at index 4
            cores = int(parts[4])
            mem = int(parts[5])
            disk = int(parts[6])
        else:
            # Without curStartTime, resources start at index 3
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])
        return {
            'type': stype,
            'id': sid,
            'state': state,
            'cores': cores,
            'mem': mem,
            'disk': disk,
            'raw': line,
        }
    # Fallback minimal parse (type id ... cores mem disk at the end)
    return None

def recv_data_block(sock_file_obj, n: int):
    # Acknowledge header
    sock_file_obj.write(b"OK\n")
    sock_file_obj.flush()
    lines = []
    for _ in range(n):
        lines.append(sock_file_obj.readline().decode().strip())
    # Acknowledge data
    sock_file_obj.write(b"OK\n")
    sock_file_obj.flush()
    # Consume terminating '.'
    sock_file_obj.readline()
    return lines

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
hourly_rate_by_type = {}  # server type -> hourly rate for tie-breaks
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
        hourly_rate_by_type[stype] = hourly_rate
        for sid in range(limit):
            info = ServerInfo(stype, sid, cores, memory, disk, boot_time, hourly_rate)
            servers.append(info)
            server_states[(stype, sid)] = ServerState(info)
except Exception as e:
    # If parsing fails, proceed without system info (fallback to on-the-fly GETS if needed)
    print(f"Warning: Could not parse ds-system.xml ({e}). Will rely on GETS Capable for server info.", file=sys.stderr)
    servers = []
    server_states = {}

last_used_tick = defaultdict(lambda: -10**9)  # (type,id) -> last assignment tick

def choose_server_for_job(job_id: int, submit_time: int, cores_req: int, mem_req: int, disk_req: int, est_runtime: int):
    """Select the best server using GETS Avail/Capable + MCT with cost- and LRU-aware tie-breaks."""
    # 1) Prefer READY (Avail) servers via GETS
    send(f"GETS Avail {cores_req} {mem_req} {disk_req}")
    header = recv()
    avail_records = []
    if header.startswith("DATA"):
        try:
            n = int(header.split()[1])
        except Exception:
            n = 0
        if n > 0:
            lines = recv_data_block(sock_file, n)
            avail_records = [parse_server_record(l) for l in lines]
        else:
            # still need to complete the '.' handshake even for zero
            sock_file.write(b"OK\n"); sock_file.flush(); sock_file.readline()
    # Selection among Avail: best-fit cores then lower hourly rate then LRU
    def cost_of(stype: str) -> float:
        return hourly_rate_by_type.get(stype, 1e9)

    def score_avail(rec):
        # Smaller (cores - req) is better; tie by cost then older last_used
        fit = rec['cores'] - cores_req
        return (
            (fit if fit >= 0 else 10**9),
            cost_of(rec['type']),
            last_used_tick[(rec['type'], rec['id'])]
        )

    chosen = None
    if avail_records:
        # Filter to those that can fit now
        feasible = [r for r in avail_records if r and r['cores'] >= cores_req and r['mem'] >= mem_req and r['disk'] >= disk_req]
        if feasible:
            feasible.sort(key=score_avail)
            r = feasible[0]
            info = ServerInfo(r['type'], r['id'], r['cores'], r['mem'], r['disk'], 0, cost_of(r['type']))
            server_states.setdefault((info.name, info.id), ServerState(info))
            return info, submit_time

    # 2) Otherwise, consider Capable servers and use MCT with our local state to predict
    send(f"GETS Capable {cores_req} {mem_req} {disk_req}")
    header = recv()
    capable_records = []
    if header.startswith("DATA"):
        try:
            n = int(header.split()[1])
        except Exception:
            n = 0
        if n > 0:
            lines = recv_data_block(sock_file, n)
            capable_records = [parse_server_record(l) for l in lines]
        else:
            sock_file.write(b"OK\n"); sock_file.flush(); sock_file.readline()

    best_server = None
    best_ready_time = None
    earliest_finish = float('inf')
    job_specs = (cores_req, mem_req, disk_req, est_runtime)
    for r in capable_records:
        if not r:
            continue
        # Ensure a ServerState exists with the capacity from system.xml if available, else from record
        key = (r['type'], r['id'])
        if key not in server_states:
            # Look up from parsed system info for accurate boot time and hourly rate
            boot = 0
            hourly = hourly_rate_by_type.get(r['type'], 0.0)
            info = ServerInfo(r['type'], r['id'], r['cores'], r['mem'], r['disk'], boot, hourly)
            server_states[key] = ServerState(info)
        state = server_states[key]
        ready_time, finish_time = state.predict_ready_time(job_specs, submit_time)
        # Primary: earliest finish; tie: lower cost; secondary: older last-used to spread load
        if (
            finish_time < earliest_finish or
            (
                finish_time == earliest_finish and best_server is not None and
                state.info.hourly_rate < best_server.hourly_rate
            ) or (
                finish_time == earliest_finish and best_server is not None and
                state.info.hourly_rate == best_server.hourly_rate and
                last_used_tick[key] < last_used_tick[(best_server.name, best_server.id)]
            )
        ):
            earliest_finish = finish_time
            best_server = state.info
            best_ready_time = ready_time

    if best_server is not None:
        return best_server, best_ready_time

    # 3) Absolute fallback: pick the largest-core server from our catalog
    fallback = None
    for info in servers:
        if info.cores >= cores_req and info.memory >= mem_req and info.disk >= disk_req:
            if fallback is None or info.cores > fallback.cores:
                fallback = info
    return fallback, submit_time if fallback else (None, None)

# Main scheduling loop
send("REDY")
current_time = 0  # track current simulation time
tick = 0          # count of scheduled jobs for LRU
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
        finish_time = server_states[(server_info.name, server_info.id)].schedule_job(
            job_id,
            (cores_req, mem_req, disk_req, runtime),
            start_time
        )
        # Send SCHD (schedule) command to server: "SCHD jobID serverType serverID"
        send(f"SCHD {job_id} {server_info.name} {server_info.id}")
        # Expect an "OK" acknowledgment for the scheduling action
        ack = recv()
        if ack != "OK":
            print(f"Protocol warning: expected OK after SCHD, got '{ack}'", file=sys.stderr)
        # After scheduling, loop back to send REDY for the next event
        last_used_tick[(server_info.name, server_info.id)] = tick
        tick += 1
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
