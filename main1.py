import multiprocessing
import os
import threading
import time

def download_simulation(file_name: str, size_mb: float) -> dict:
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Початок завантаження '{file_name}' ({size_mb} MB)...")
    time.sleep(size_mb * 0.3)
    print(f"[{thread_name}] Завершено '{file_name}'")
    return {"file": file_name, "size": size_mb, "thread": thread_name}

class MonitorThread(threading.Thread):
    def __init__(self, interval: float = 0.5):
        super().__init__(name="Monitor", daemon=True)
        self.interval = interval
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.is_set():
            print(f"[Монітор] Кількість активних потоків: {threading.active_count()}")
            time.sleep(self.interval)

    def stop(self):
        self._stop_event.set()

def task_1_1():
    print("Завдання 1.1: Робота з потоками (threading)")

    files = [
        ("data.csv", 1.5),
        ("report.pdf", 0.8),
        ("image.png", 0.5),
        ("archive.zip", 2.0),
        ("log.txt", 0.3),
    ]

    print("\nПослідовне завантаження:")
    seq_start = time.perf_counter()
    for name, size in files:
        download_simulation(name, size)
    seq_time = time.perf_counter() - seq_start
    print(f"Послідовний час: {seq_time:.2f} сек")

    print("\nПаралельне завантаження через потоки:")
    monitor = MonitorThread(interval=0.4)
    monitor.start()

    threads = []
    par_start = time.perf_counter()

    for name, size in files:
        t = threading.Thread(target=download_simulation, args=(name, size))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    par_time = time.perf_counter() - par_start
    monitor.stop()

    print(f"\nЧас роботи потоків: {par_time:.2f} сек")
    print(f"Прискорення: {seq_time / par_time:.2f}x")


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

def sum_primes_in_range(start: int, end: int) -> tuple[int, int, int]:
    pid = os.getpid()
    total_sum = 0
    count = 0
    for num in range(start, end):
        if is_prime(num):
            total_sum += num
            count += 1
    return total_sum, count, pid

def task_1_2():
    print("\nЗавдання 1.2: Робота з процесами (multiprocessing)")

    total_range = 500_000
    num_workers = 4
    chunk_size = total_range // num_workers
    ranges = []

    for i in range(num_workers):
        start = i * chunk_size if i > 0 else 2
        end = (i + 1) * chunk_size
        ranges.append((start, end))

    print(f"Діапазони для обчислення: {ranges}")

    print("\nПослідовний розрахунок:")
    seq_start = time.perf_counter()
    seq_results = [sum_primes_in_range(r[0], r[1]) for r in ranges]
    seq_time = time.perf_counter() - seq_start
    seq_sum = sum(res[0] for res in seq_results)
    print(f"Сума простих чисел: {seq_sum}")
    print(f"Послідовний час: {seq_time:.3f} сек")

    print("\nПаралельний розрахунок через multiprocessing.Pool:")
    par_start = time.perf_counter()
    with multiprocessing.Pool(processes=num_workers) as pool:
        par_results = pool.starmap(sum_primes_in_range, ranges)
    par_time = time.perf_counter() - par_start

    par_sum = sum(res[0] for res in par_results)
    pids = [res[2] for res in par_results]

    print(f"Сума простих чисел: {par_sum}")
    print(f"PID задіяних процесів: {pids}")
    print(f"Паралельний час: {par_time:.3f} сек")
    print(f"Прискорення: {seq_time / par_time:.2f}x")

if __name__ == "__main__":
    task_1_1()
    task_1_2()
