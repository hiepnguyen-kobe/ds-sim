#!/usr/bin/env python3
import os, socket, argparse, math
from collections import defaultdict

BUF = 4096

def send(sock, msg): sock.sendall((msg + "\n").encode())
def recv(sock): return sock.recv(BUF).decode().strip()

def recv_block(sock, n):
    # DATA n ...  => OK => (n lines) => OK => '.'
    send(sock, "OK")
    lines = [recv(sock) for _ in range(n)]
    send(sock, "OK")
    recv(sock)        # consume '.'
    return lines

def parse_server(line):
    p = line.split()
    return {
        "type":  p[0],
        "id":    int(p[1]),
        "state": p[2],
        "cores": int(p[4]),
        "mem":   int(p[5]),
        "disk":  int(p[6]),
        "raw":   line,
    }

def get_list(sock, query):
    send(sock, query)
    hdr = recv(sock)                       # e.g., DATA 3 124
    if not hdr.startswith("DATA"): return []
    n = int(hdr.split()[1])
    return [parse_server(x) for x in recv_block(sock, n)]

# ---------- LRUBF selection ----------
def best_fit_with_lru(servers, job_cores, last_used, tick):
    """
    Among 'servers' choose the one with:
      1) minimal (cores - job_cores) >= 0
      2) tie-break: smallest last_used timestamp (least recently used)
    'last_used' maps (type,id)->tick of last assignment (default = -inf).
    """
    choice = None
    best_fit = math.inf
    best_lru = math.inf
    for s in servers:
        fit = s["cores"] - job_cores
        if fit < 0:           # can't run at all
            continue
        lru_key = (s["type"], s["id"])
        lru_val = last_used.get(lru_key, -10**9)
        # primary: smaller fit; secondary: older (smaller) last_used
        if (fit < best_fit) or (fit == best_fit and lru_val < best_lru):
            best_fit = fit
            best_lru = lru_val
            choice = s
    return choice

def schedule_one(sock, job_id, c, m, d, last_used, tick):
    # 1) try ready servers (can start now)
    avail = get_list(sock, f"GETS Avail {c} {m} {d}")
    ch = best_fit_with_lru(avail, c, last_used, tick) if avail else None

    # 2) otherwise pick best-fit+LRU among capable (will queue)
    if ch is None:
        cap = get_list(sock, f"GETS Capable {c} {m} {d}")
        ch = best_fit_with_lru(cap, c, last_used, tick) if cap else None

    # 3) safety ATL if still none
    if ch is None:
        allsrv = get_list(sock, "GETS All")
        if allsrv:
            ch = max(allsrv, key=lambda s: s["cores"])
        else:
            return None  # nothing to do (keep protocol alive outside)

    send(sock, f"SCHD {job_id} {ch['type']} {ch['id']}")
    recv(sock)  # OK
    last_used[(ch["type"], ch["id"])] = tick
    return ch

def main(host, port, user):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    # handshake
    send(s, "HELO"); recv(s)
    send(s, f"AUTH {user}"); recv(s)

    last_used = defaultdict(lambda: -10**9)  # (type,id) -> last assignment tick
    tick = 0                                 # increases every scheduled job

    send(s, "REDY")
    msg = recv(s)

    while msg != "NONE":
        if msg.startswith("JOBN"):
            parts = msg.split()
            jid = parts[2]
            c, m, d = map(int, parts[4:7])

            # choose & schedule using LRUBF
            ch = schedule_one(s, jid, c, m, d, last_used, tick)
            if ch is not None:
                tick += 1
            else:
                # couldn’t pick (empty lists) — just advance protocol
                pass

        send(s, "REDY")
        msg = recv(s)

    send(s, "QUIT"); recv(s)
    s.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-H","--host", default="localhost")
    ap.add_argument("-p","--port", type=int, default=50000)
    ap.add_argument("-u","--user", default=(os.environ.get("USER") or "student"))
    args = ap.parse_args()
    main(args.host, args.port, args.user)

