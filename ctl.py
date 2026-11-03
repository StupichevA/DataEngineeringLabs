import os
import sys
import json
import time
import psutil
from kazoo.client import KazooClient

HOSTS = os.getenv("ZK_HOSTS", "localhost:2181,localhost:2182,localhost:2183")
HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, ".pause_state.json")


def now():
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + f".{int(t * 1000) % 1000:03d}"


def event(run, text):
    os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)
    line = f"{now()} {text}"
    print(line)
    with open(os.path.join(HERE, "logs", f"{run}-pause.log"), "a", encoding="utf-8") as f:
        f.write(line + "\n")


def zk_start():
    zk = KazooClient(hosts=HOSTS, timeout=10)
    zk.start(timeout=10)
    return zk


def main():
    cmd = sys.argv[1]
    run = sys.argv[2] if len(sys.argv) > 2 else "run"

    if cmd == "pause":
        zk = zk_start()
        info = json.loads(zk.get("/mr/leader")[0])
        zk.stop(); zk.close()
        parent = psutil.Process(info["pid"])
        children = parent.children(recursive=True)
        t = time.time()
        parent.suspend()                      # сначала родитель: от этого момента нет heartbeat
        for c in children:
            c.suspend()
        json.dump({"pid": info["pid"], "id": info["id"], "t": t}, open(STATE, "w"))
        event(run, f"ПАУЗА лидера {info['id']} pid={info['pid']} (процессов в дереве: {1 + len(children)})")

    elif cmd == "resume":
        st = json.load(open(STATE))
        parent = psutil.Process(st["pid"])
        for c in parent.children(recursive=True):
            c.resume()
        parent.resume()
        event(run, f"ВОЗОБНОВЛЕНИЕ лидера {st['id']} pid={st['pid']}, простоял {time.time() - st['t']:.1f} с")

    elif cmd == "show":
        zk = zk_start()
        data, stat = zk.get("/mr/pointer")
        event(run, f"УКАЗАТЕЛЬ: версия={stat.version}, данные={data.decode() or '(пусто)'}")
        zk.stop(); zk.close()


if __name__ == "__main__":
    main()