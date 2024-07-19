from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import psutil  # External library for system-related information

# Function to test worker functionality (a simple example task)
def test_worker(worker_id):
    time.sleep(1)  # Simulate work with a delay
    return worker_id

def get_max_thread_count():
    # Using psutil to get the number of logical CPUs
    return psutil.cpu_count(logical=True)

def main():
    # Get the maximum number of threads supported
    max_threads = get_max_thread_count()
    
    # For demonstration, we will use a lower number if max_threads is too high
    max_workers = min(max_threads, 32)  # Limit to a reasonable max of 32 for this example
    
    print(f"Maximum number of threads supported by system: {max_threads}")
    print(f"Using {max_workers} workers for this test")


    print("All tasks completed")

if __name__ == "__main__":
    main()
