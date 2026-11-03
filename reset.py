import os
import shutil
from kazoo.client import KazooClient
import steps

HERE = os.path.dirname(os.path.abspath(__file__))
zk = KazooClient(hosts=os.getenv("ZK_HOSTS", "localhost:2181,localhost:2182,localhost:2183"))
zk.start(timeout=10)
if zk.exists("/mr"):
    zk.delete("/mr", recursive=True)
zk.stop(); zk.close()
shutil.rmtree(steps.WORK_DIR, ignore_errors=True)
shutil.rmtree(os.path.join(HERE, "results"), ignore_errors=True)
if os.path.exists(os.path.join(HERE, ".pause_state.json")):
    os.remove(os.path.join(HERE, ".pause_state.json"))
print("сброшено: /mr в ZooKeeper, чекпоинты, results")