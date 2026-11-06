import random
import threading
import time

class BankAccount:
    def __init__(self, balance: float = 0.0):
        self.balance = balance
        self.transaction_count = 0

    def deposit(self, amount: float) -> None:
        current = self.balance
        time.sleep(random.uniform(0.0001, 0.0005))
        self.balance = current + amount
        self.transaction_count += 1

    def withdraw(self, amount: float) -> bool:
        current = self.balance
        time.sleep(random.uniform(0.0001, 0.0005))
        if current >= amount:
            self.balance = current - amount
            self.transaction_count += 1
            return True
        return False

class SafeBankAccount:
    def __init__(self, balance: float = 0.0):
        self.balance = balance
        self.transaction_count = 0
        self._lock = threading.Lock()

    def deposit(self, amount: float) -> None:
        with self._lock:
            current = self.balance
            time.sleep(random.uniform(0.0001, 0.0005))
            self.balance = current + amount
            self.transaction_count += 1

    def withdraw(self, amount: float) -> bool:
        with self._lock:
            current = self.balance
            time.sleep(random.uniform(0.0001, 0.0005))
            if current >= amount:
                self.balance = current - amount
                self.transaction_count += 1
                return True
            return False

def run_transactions(account, num_deposits: int, num_withdrawals: int, dep_amount: float, with_amount: float):
    threads = []
    dep_per_thread = num_deposits // 5
    with_per_thread = num_withdrawals // 5

    def deposit_worker():
        for _ in range(dep_per_thread):
            account.deposit(dep_amount)

    def withdraw_worker():
        for _ in range(with_per_thread):
            account.withdraw(with_amount)

    for _ in range(5):
        threads.append(threading.Thread(target=deposit_worker))
    for _ in range(5):
        threads.append(threading.Thread(target=withdraw_worker))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

def task_2_1():
    print("Завдання 2.1: Гонка даних та Lock")

    initial_balance = 1000.0
    num_deposits = 100
    deposit_amount = 50.0
    num_withdrawals = 100
    withdraw_amount = 50.0

    expected_balance = (
        initial_balance
        + num_deposits * deposit_amount
        - num_withdrawals * withdraw_amount
    )

    print("\nТест 1: Без синхронізації (BankAccount)")
    unsafe_account = BankAccount(initial_balance)
    run_transactions(unsafe_account, num_deposits, num_withdrawals, deposit_amount, withdraw_amount)

    print(f"Очікуваний баланс: {expected_balance:.2f}")
    print(f"Фактичний баланс:  {unsafe_account.balance:.2f}")
    print(f"Кількість транзакцій: {unsafe_account.transaction_count}")
    print(f"Збігається: {unsafe_account.balance == expected_balance}")

    print("\nТест 2: Із синхронізацією через Lock (SafeBankAccount)")
    safe_account = SafeBankAccount(initial_balance)
    run_transactions(safe_account, num_deposits, num_withdrawals, deposit_amount, withdraw_amount)

    print(f"Очікуваний баланс: {expected_balance:.2f}")
    print(f"Фактичний баланс:  {safe_account.balance:.2f}")
    print(f"Кількість транзакцій: {safe_account.transaction_count}")
    print(f"Збігається: {safe_account.balance == expected_balance}")

class ConnectionPool:
    def __init__(self, max_connections: int = 3):
        self._semaphore = threading.Semaphore(max_connections)
        self._lock = threading.Lock()
        self.max_connections = max_connections
        self.active_connections = 0
        self.total_served = 0
        self.peak_connections = 0

    def acquire_connection(self, client_name: str) -> None:
        print(f"[{client_name}] Запит на з'єднання...")
        self._semaphore.acquire()
        with self._lock:
            self.active_connections += 1
            if self.active_connections > self.peak_connections:
                self.peak_connections = self.active_connections

    def release_connection(self, client_name: str) -> None:
        with self._lock:
            self.active_connections -= 1
            self.total_served += 1
        self._semaphore.release()

    def get_stats(self) -> dict:
        return {
            "max": self.max_connections,
            "active": self.active_connections,
            "total_served": self.total_served,
            "peak": self.peak_connections,
        }

def client_task(pool: ConnectionPool, client_name: str, ready_event: threading.Event) -> None:
    ready_event.wait()
    pool.acquire_connection(client_name)
    try:
        work_time = random.uniform(0.1, 0.3)
        print(f"[{client_name}] Працює {work_time:.2f} сек (активних з'єднань: {pool.active_connections})")
        time.sleep(work_time)
    finally:
        pool.release_connection(client_name)

def task_2_2():
    print("\nЗавдання 2.2: Семафори та Event")

    pool = ConnectionPool(max_connections=3)
    ready_event = threading.Event()
    num_clients = 10

    threads = []
    for i in range(num_clients):
        t = threading.Thread(
            target=client_task,
            args=(pool, f"Client-{i:02d}", ready_event)
        )
        threads.append(t)
        t.start()

    time.sleep(0.3)
    print("Сервер готовий! Усі клієнти починають роботу.\n")
    ready_event.set()

    for t in threads:
        t.join()

    stats = pool.get_stats()
    print("\nСтатистика пулу з'єднань:")
    print(f"Макс. з'єднань дозволено: {stats['max']}")
    print(f"Обслуговано клієнтів:     {stats['total_served']}")
    print(f"Пікове навантаження:       {stats['peak']}")
    print(f"Активних зараз:            {stats['active']}")

if __name__ == "__main__":
    task_2_1()
    task_2_2()
