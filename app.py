import json
import os
import time
import threading
import hashlib
import ray
from pcfg_lib.guess.pcfg.pcfg_guesser import PCFGGuesser
from pcfg_lib.guess.util.priority_queue import PcfgQueue
from ray import runtime_context
from ray._private.services import get_node_ip_address


# ──────────────────────────────────── MainNode ────────────────────────────────────
@ray.remote
class MainNode:
    def __init__(self, config: dict):
        # 설정 보관만: 실제 PCFG 초기화는 별도 메서드로 지연 수행
        self.config = config
        self.pcfg = None
        self.queue = None

        # 상태 변수
        self.seed_counter = 0
        self.active_tasks = {}
        self._lock = threading.Lock()
        self.guess_rate = self.compare_rate = 0
        self.guess_workers = []
        self.compare_workers = []
        self.target_hash = "F0B55E2312C383B8C9DCDD0C875C42AA7012AB96025D744091318CE00EB80569"
        self._shutdown_event = threading.Event()

        print(f"[MainNode] actor initialized", flush=True)

        # 초기화 작업: PCFGGuesser 생성 등 지연 수행
        threading.Thread(target=self._initialize_heavy, daemon=True).start()
        threading.Thread(target=self._auto_scale_loop, daemon=True).start()

    def _initialize_heavy(self):
        # Actor ready 확인 후에 무거운 초기화
        time.sleep(1)
        # PCFGGuesser 및 큐 초기화
        self.pcfg = PCFGGuesser(self.config)
        self.queue = PcfgQueue(self.pcfg)
        print(f"[MainNode] PCFG & queue ready, roots={len(self.queue._heap)}", flush=True)

        # 초기 워커 부트스트랩
        for _ in range(2):
            gw = GuessWorker.options(num_cpus=1).remote()
            self.register_guess_worker(gw)
            gw.run.remote()
        cw = CompareWorker.options(num_cpus=0.5).remote()
        self.register_compare_worker(cw)

    # ───────── 큐 & 통계 ─────────
    def next_seed(self):
        sid, self.seed_counter = self.seed_counter, self.seed_counter + 1
        return sid

    def get_task_batch(self, n=1):
        # Heavy init이 안 끝나면 빈 배치 반환
        if self.queue is None:
            return []
        batch = []
        for _ in range(n):
            node = self.queue.pop()
            if not node:
                break
            sid = self.next_seed()
            self.active_tasks[sid] = node
            batch.append((sid, node))
        print(batch, flush=True)
        return batch

    def report_children(self, sid, children):
        for c in children:
            self.queue.push(c)
        self.active_tasks.pop(sid, None)

    # ───────── 결과/통계 ─────────
    def submit_guess(self, sid, guess):
        with self._lock:
            self.guess_rate += 1
        if self.compare_workers and self.pcfg:
            self.compare_workers[sid % len(self.compare_workers)].compare.remote(
                sid, guess, self.target_hash
            )

    def report_compare_done(self):
        with self._lock:
            self.compare_rate += 1

    def submit_result(self, sid, guess):
        print(f"[MainNode] FOUND seed={sid} → {guess}", flush=True)
        self._shutdown_event.set()
        for a in (*self.guess_workers, *self.compare_workers):
            a.shutdown.remote()

    def is_alive(self):
        return not self._shutdown_event.is_set()

    # ───────── 오토 스케일 ─────────
    def _auto_scale_loop(self):
        from time import sleep
        while not self._shutdown_event.is_set():
            sleep(1)
            with self._lock:
                backlog = self.guess_rate - self.compare_rate
                self.guess_rate = self.compare_rate = 0
            if backlog > 100 and len(self.compare_workers) < 10:
                cw = CompareWorker.options(num_cpus=0.5).remote()
                self.register_compare_worker(cw)
            elif backlog < -50 and len(self.guess_workers) < 20:
                gw = GuessWorker.options(num_cpus=1).remote()
                self.register_guess_worker(gw)
                gw.run.remote()

    # ───────── 워커 등록 API ─────────
    def register_guess_worker(self, actor_handle):
        self.guess_workers.append(actor_handle)

    def register_compare_worker(self, actor_handle):
        self.compare_workers.append(actor_handle)

    def ping(self):
        return True

# ─────────────────────────────── GuessWorker ───────────────────────────────
@ray.remote
class GuessWorker:
    def __init__(self):
        self.main = ray.get_actor("MainNode", namespace="pcfg")
        self.pcfg = None
        self.running = True

    def run(self):
        pid = os.getpid()
        # 지연 초기화: 메인 노드 준비 대기
        while self.pcfg is None:
            try:
                self.pcfg = PCFGGuesser({"log": False})
            except Exception:
                time.sleep(0.1)
        print(f"[GuessWorker][{pid}] start", flush=True)
        while self.running:
            try:
                batch = ray.get(self.main.get_task_batch.remote(n=2))
                if not batch:
                    time.sleep(0.05)
                    continue
                for sid, node in batch:
                    self.main.report_children.remote(sid, self.pcfg.find_children(node))
                    for chunk in self.pcfg.split_structures(node, value=1):
                        print(json.dumps({
                            "event": "password_guessed",
                            "input": get_node_ip_address(),
                            "guess": [structure.serialize() for structure in chunk],
                        }), flush=True)
                        for pw in self.pcfg.guess(chunk):
                            self.main.submit_guess.remote(sid, pw)
            except ray.exceptions.RayActorError:
                print(f"[GuessWorker][{pid}] MainNode gone → exit", flush=True)
                break

    def shutdown(self):
        self.running = False

# ─────────────────────────────── CompareWorker ───────────────────────────────
@ray.remote
class CompareWorker:
    def __init__(self):
        self.main = ray.get_actor("MainNode", namespace="pcfg")
        self.running = True

    def compare(self, sid, guess, target_hash):
        if not self.running:
            return
        if hashlib.sha256(guess.encode()).hexdigest() == target_hash:
            self.main.submit_result.remote(sid, guess)
        else:
            self.main.report_compare_done.remote()

    def shutdown(self):
        self.running = False

# ─────────────────────────────── Driver ───────────────────────────────
if __name__ == "__main__":
    ray.init(
        address="ray://cifrar.cju.ac.kr:25577",
        namespace="pcfg",
        runtime_env={"env_vars": {"RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1"}},
    )

    target_hash = os.getenv("TARGET_HASH", "<sha256_target>")

    # MainNode actor 확보 또는 생성
    try:
        main = ray.get_actor("MainNode", namespace="pcfg")
        print("[Driver] reuse MainNode")
    except ValueError:
        main = MainNode.options(
            name="MainNode", namespace="pcfg", lifetime="detached",
            resources={"main_node_host": 0.001}, num_cpus=1,
            memory=8 * 1024**3,
            max_restarts=1,
            max_task_retries=1
        ).remote({"target_hash": target_hash})
        ray.get(main.ping.remote())
        print("[Driver] new MainNode created")

    try:
        while ray.get(main.is_alive.remote()):
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Driver] Ctrl‑C → kill MainNode")
        ray.kill(main, no_restart=True)

    ray.shutdown()
