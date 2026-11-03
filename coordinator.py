import os
import sys
import json
import time
import logging
from kazoo.client import KazooClient
from kazoo.recipe.election import Election
from kazoo.exceptions import NodeExistsError, ConnectionLoss, SessionExpiredError

from steps import run_mapreduce_salary, control_numbers

HOSTS = os.getenv("ZK_HOSTS", "localhost:2181,localhost:2182,localhost:2183")
SESSION_TIMEOUT = 10
POINTER = "/mr/pointer"
LEADER = "/mr/leader"
HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, ".pause_state.json")

ID = sys.argv[1] if len(sys.argv) > 1 else "A"
RUN = sys.argv[2] if len(sys.argv) > 2 else "run"
K = int(sys.argv[3]) if len(sys.argv) > 3 else 2
FENCING = os.getenv("FENCING") == "1"


def say(msg):
    logging.getLogger("coord").info(msg)


def setup_logging():
    sys.stdout.reconfigure(encoding="utf-8")
    os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)
    fmt = logging.Formatter(f"%(asctime)s.%(msecs)03d [{ID}] %(message)s", "%H:%M:%S")
    lg = logging.getLogger("coord")
    lg.setLevel(logging.INFO)
    for h in (logging.FileHandler(os.path.join(HERE, "logs", f"{RUN}-{ID}.log"), "w", encoding="utf-8"),
              logging.StreamHandler()):
        h.setFormatter(fmt)
        lg.addHandler(h)


def publish(zk, payload, epoch):
    """FENCING=1: запись только если версия указателя всё ещё равна эпохе, взятой при получении лидерства."""
    for _ in range(15):
        try:
            if FENCING:
                return zk.set(POINTER, payload, version=epoch).version
            return zk.set(POINTER, payload).version
        except (ConnectionLoss, SessionExpiredError) as e:
            say(f"публикация: {type(e).__name__}, повтор через 1 с")
            time.sleep(1)
    raise RuntimeError("не удалось связаться с ZooKeeper")


def make_lead(zk):
    def lead():
        data, stat = zk.get(POINTER)
        if data:
            say(f"результат уже опубликован ({data.decode()}), выхожу")
            return
        epoch = stat.version  # эпоха = версия указателя на момент получения лидерства
        say(f"СТАЛ ЛИДЕРОМ, эпоха={epoch}, pid={os.getpid()}, FENCING={int(FENCING)}")
        if os.path.exists(STATE):
            t0 = json.load(open(STATE))["t"]
            say(f"смена лидера: прошло {time.time() - t0:.1f} с после приостановки предыдущего лидера")

        info = json.dumps({"id": ID, "pid": os.getpid()}).encode()
        try:
            zk.create(LEADER, info, ephemeral=True)
        except NodeExistsError:
            zk.set(LEADER, info)

        df = run_mapreduce_salary(K, resume=True, log=say)
        ctrl = control_numbers(df)
        say(f"расчёт завершён, контрольные числа: {ctrl}")

        os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
        path = os.path.join("results", f"{RUN}-result-{ID}.csv")
        df.to_csv(os.path.join(HERE, path), index=False)

        payload = json.dumps({"writer": ID, "file": path, "epoch": epoch, "control": ctrl},
                             ensure_ascii=False).encode()
        try:
            version = publish(zk, payload, epoch)
        except Exception as e:
            if type(e).__name__ == "BadVersionError":
                say(f"BadVersionError: запись отклонена (моя эпоха {epoch}, указатель уже изменён), "
                    f"результат не публикую")
                return
            raise
        say(f"записал результат: файл {path}, версия указателя {version}")
    return lead


def main():
    setup_logging()
    zk = KazooClient(hosts=HOSTS, timeout=SESSION_TIMEOUT)
    zk.add_listener(lambda state: say(f"состояние сессии ZooKeeper: {state}"))
    zk.start(timeout=15)
    say(f"сессия {hex(zk.client_id[0])}, таймаут {SESSION_TIMEOUT} с, pid={os.getpid()}")

    zk.ensure_path("/mr")
    try:
        zk.create(POINTER, b"")
    except NodeExistsError:
        pass

    say("участвую в выборах, жду лидерства...")
    try:
        Election(zk, "/mr/election", ID).run(make_lead(zk))
    except Exception as e:
        say(f"завершение: {type(e).__name__}: {e}")
    finally:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass
    say("процесс завершён")


if __name__ == "__main__":
    main()