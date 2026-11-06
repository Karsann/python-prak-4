from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import hashlib
import queue
import random
import threading
import time

def io_bound_task(url: str) -> dict:
    time.sleep(0.1)
    return {"url": url, "status": 200, "size": len(url) * 42}

def cpu_bound_task(n: int) -> dict:
    hasher = hashlib.sha256()
    for i in range(n):
        hasher.update(str(i).encode())
    return {"n": n, "hash": hasher.hexdigest()[:16]}

def risky_task(value: int) -> float:
    if value == 0:
        raise ZeroDivisionError("Ділення на нуль!")
    if value < 0:
        raise ValueError(f"Від'ємне значення: {value}")
    return 1000.0 / value

def benchmark_io_bound():
    urls = [f"https://api.example.com/data/{i}" for i in range(15)]

    start = time.perf_counter()
    for u in urls:
        io_bound_task(u)
    seq_time = time.perf_counter() - start

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(io_bound_task, urls))
    par_time = time.perf_counter() - start

    print(f"Послідовний час I/O: {seq_time:.2f} сек")
    print(f"Паралельний час I/O: {par_time:.2f} сек")
    print(f"Прискорення: {seq_time / par_time:.2f}x")

def benchmark_cpu_bound():
    tasks = [250_000] * 6

    start = time.perf_counter()
    for t in tasks:
        cpu_bound_task(t)
    seq_time = time.perf_counter() - start

    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(executor.map(cpu_bound_task, tasks))
    par_time = time.perf_counter() - start

    print(f"Послідовний час CPU: {seq_time:.2f} сек")
    print(f"Паралельний час CPU: {par_time:.2f} сек")
    print(f"Прискорення: {seq_time / par_time:.2f}x")

def handle_exceptions():
    values = [10, 5, 0, -3, 8, 2]
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(risky_task, val): val for val in values}
        for future in as_completed(futures):
            val = futures[future]
            try:
                res = future.result()
                print(f"Значення {val} успішно обчислено: {res:.2f}")
            except (ZeroDivisionError, ValueError) as err:
                print(f"Значення {val} викликало помилку: {err}")

def task_3_1():
    print("Завдання 3.1: Пули потоків і процесів (concurrent.futures)")
    print("\nТест I/O-bound задач (ThreadPoolExecutor):")
    benchmark_io_bound()

    print("\nТест CPU-bound задач (ProcessPoolExecutor):")
    benchmark_cpu_bound()

    print("\nОбробка виключень у пулі:")
    handle_exceptions()

class Task:
    def __init__(self, task_id: int, data: str, priority: int = 0):
        self.task_id = task_id
        self.data = data
        self.priority = priority
        self.created_at = time.time()

    def __repr__(self):
        return f"Task(id={self.task_id}, data='{self.data}')"

def producer(task_queue: queue.Queue, num_tasks: int, producer_id: int):
    for i in range(num_tasks):
        t = Task(
            task_id=producer_id * 100 + i,
            data=f"Пакет {i} від виробника {producer_id}",
            priority=random.randint(1, 5)
        )
        task_queue.put(t)
        print(f"[Виробник {producer_id}] Створив: {t}")
        time.sleep(random.uniform(0.02, 0.05))

def consumer(task_queue: queue.Queue, consumer_id: int, results: list, results_lock: threading.Lock):
    while True:
        try:
            task = task_queue.get(timeout=2)
        except queue.Empty:
            print(f"[Споживач {consumer_id}] Черга порожня, завершення")
            break

        if task is None:
            task_queue.put(None)
            break

        time.sleep(random.uniform(0.03, 0.06))
        with results_lock:
            results.append(task.task_id)
        task_queue.task_done()
        print(f"[Споживач {consumer_id}] Обробив задачу id={task.task_id}")

def producer_consumer_demo():
    task_queue = queue.Queue(maxsize=5)
    results = []
    results_lock = threading.Lock()

    p_threads = []
    for p_id in range(2):
        t = threading.Thread(target=producer, args=(task_queue, 4, p_id + 1))
        p_threads.append(t)
        t.start()

    c_threads = []
    for c_id in range(3):
        t = threading.Thread(target=consumer, args=(task_queue, c_id + 1, results, results_lock))
        c_threads.append(t)
        t.start()

    for t in p_threads:
        t.join()

    task_queue.put(None)

    for t in c_threads:
        t.join()

    print(f"\nУсього успішно оброблено задач: {len(results)}")

def deadlock_demo():
    lock_a = threading.Lock()
    lock_b = threading.Lock()
    deadlock_detected = threading.Event()

    def worker_1():
        lock_a.acquire()
        print("[Worker 1] Захопив Lock A")
        time.sleep(0.05)
        acquired = lock_b.acquire(timeout=1.0)
        if not acquired:
            print("[Worker 1] Дедлок: не зміг захопити Lock B за таймаутом")
            deadlock_detected.set()
            lock_a.release()
            return
        lock_b.release()
        lock_a.release()

    def worker_2():
        lock_b.acquire()
        print("[Worker 2] Захопив Lock B")
        time.sleep(0.05)
        acquired = lock_a.acquire(timeout=1.0)
        if not acquired:
            print("[Worker 2] Дедлок: не зміг захопити Lock A за таймаутом")
            deadlock_detected.set()
            lock_b.release()
            return
        lock_a.release()
        lock_b.release()

    def safe_worker_1():
        with lock_a:
            time.sleep(0.02)
            with lock_b:
                print("[Safe Worker 1] Успішно захопив A -> B")

    def safe_worker_2():
        with lock_a:
            time.sleep(0.02)
            with lock_b:
                print("[Safe Worker 2] Успішно захопив A -> B")

    print("\nСпроба з потенційним дедлоком (різний порядок замків):")
    t1 = threading.Thread(target=worker_1)
    t2 = threading.Thread(target=worker_2)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    if deadlock_detected.is_set():
        print("Дедлок успішно виявлено і розірвано завдяки таймауту")

    print("\nБезпечна версія (однаковий порядок A -> B):")
    st1 = threading.Thread(target=safe_worker_1)
    st2 = threading.Thread(target=safe_worker_2)
    st1.start()
    st2.start()
    st1.join()
    st2.join()
    print("Безпечні потоки завершили роботу без блокувань")

def task_3_2():
    print("\nЗавдання 3.2: Черги (Producer-Consumer) та дедлоки")
    print("\nПатерн Producer-Consumer:")
    producer_consumer_demo()

    print("\nДемонстрація взаємного блокування (Deadlock):")
    deadlock_demo()

if __name__ == "__main__":
    task_3_1()
    task_3_2()
