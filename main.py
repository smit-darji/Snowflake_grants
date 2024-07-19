import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Function to simulate work (replace with your actual task)
def test_worker(worker_id):
    time.sleep(1)  # Simulate some work with a delay
    return worker_id

# Function to get the maximum number of threads you can use effectively
def get_max_workers():
    # Get the number of logical CPUs
    logical_cpus = psutil.cpu_count(logical=True)
    print(f"Number of logical CPUs: {logical_cpus}")

    # Set max_workers based on the number of CPUs
    max_workers = min(logical_cpus, 32)  # Cap it at 32 for this example
    return max_workers

def main():
    max_workers = get_max_workers()

if __name__ == "__main__":
    main()
