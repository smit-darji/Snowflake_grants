import psutil
from concurrent.futures import ThreadPoolExecutor

def get_max_thread_count():
    return psutil.cpu_count(logical=True)

def main():
    max_threads = get_max_thread_count()
    print(f"Maximum number of threads supported by system: {max_threads}")

    # Create a ThreadPoolExecutor with the determined number of workers
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Example task submission and processing
        futures = [executor.submit(lambda x: x * x, i) for i in range(10)]
        results = [future.result() for future in futures]
        print("Results:", results)

if __name__ == "__main__":
    main()
