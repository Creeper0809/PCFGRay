import json
import os
import time
import threading
import hashlib
import ray
from pcfg_lib.guess.pcfg.pcfg_guesser import PCFGGuesser
from pcfg_lib.guess.util.priority_queue import PcfgQueue

# ──────────────────────────────────── MainNode ────────────────────────────────────
@ray.remote
class MainNode:
    def __init__(self, config: dict):
        # PCFG 및 큐 초기화
        self.pcfg = PCFGGuesser(config)
        self.queue = PcfgQueue(self.pcfg)

        # 상태 변수
        self.seed_counter = 0
        self.active_tasks = {}
        self._lock = threading.Lock()
        self.guess_rate = self.compare_rate = 0
        self.guess_workers = []
        self.compare_workers = []
        self.target_hash = config.get("target_hash")
        self._shutdown_event = threading.Event()

        print(f"[MainNode] roots={len(self.queue._heap)}", flush=True)

        # 오토스케일 및 부트스트랩 스레드 시작
        threading.Thread(target=self._auto_scale_loop, daemon=True).start()
        threading.Thread(target=self._delayed_bootstrap, daemon=True).start()

    # 초기 워커 부트스트랩 (Actor ready 확보 후 실행)
    def _delayed_bootstrap(self):
        time.sleep(1)
        # GuessWorker 2개
        for _ in range(2):
            gw = GuessWorker.options(
                num_cpus=1,
                memory=600 * 1024**2
            ).remote()
            # 내부 메서드로 직접 등록
            self.register_guess_worker(gw)
            gw.run.remote()
        # CompareWorker 1개
        cw = CompareWorker.options(
                    num_cpus=0.5,
                    memory=200 * 1024**2
                ).remote()
        self.register_compare_worker(cw)

    # ───────── 큐 & 통계 ─────────
    def next_seed(self):
        sid, self.seed_counter = self.seed_counter, self.seed_counter + 1
        return sid

    def get_task_batch(self, n=1):
        batch = []
        for _ in range(n):
            node = self.queue.pop()
            if not node:
                break
            sid = self.next_seed()
            self.active_tasks[sid] = node
            batch.append((sid, node))
        return batch

    def report_children(self, sid, children):
        for c in children:
            self.queue.push(c)
        self.active_tasks.pop(sid, None)

    # ───────── 결과/통계 ─────────
    def submit_guess(self, sid, guess):
        with self._lock:
            self.guess_rate += 1
        if self.compare_workers:
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
            # 비교 워커 추가
            if backlog > 100 and len(self.compare_workers) < 10:
                cw = CompareWorker.options(
                    num_cpus=0.5,
                    memory=200 * 1024**2,
                    resources={"worker_node": 0.005}
                ).remote()
                self.register_compare_worker(cw)
            # 추측 워커 추가
            elif backlog < -50 and len(self.guess_workers) < 20:
                gw = GuessWorker.options(
                    num_cpus=1,
                    memory=600 * 1024**2,
                    resources={"worker_node": 0.01}
                ).remote()
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
        # MainNode actor handle 조회
        self.main = ray.get_actor("MainNode", namespace="pcfg")
        self.pcfg = PCFGGuesser({"log": False})
        self.running = True

    def run(self):
        pid = os.getpid()
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
                        for pw in self.pcfg.guess(chunk):
                            print(json.dumps({
                                                "event": "password_guessed",
                                                "input": [structure.serialize() for structure in chunk],
                                                "guess": pw
                                                }), flush=True)
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
            resources={"main_node_host": 0.001}, num_cpus=1
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
