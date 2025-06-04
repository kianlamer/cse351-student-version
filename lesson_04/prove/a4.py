"""
Course      : CSE 351
Assignment  : 04
Student     : Kian LaMer

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0
"""

import time
from common import *
from cse351 import *
import threading # Reverted to threading
import queue     # Reverted to queue
import logging

# Configuration (Reduced THREADS for easier debugging)
THREADS = 4
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000  # Don't change

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')


# ---------------------------------------------------------------------------
def retrieve_weather_data(commands_queue, data_queue):
    """
    Retrieve weather data from the server
    :param commands_queue: queue of commands to retrieve
    :param data_queue: queue to store retrieved data
    :return:
    """

    while True:
        try:
            city, recno = commands_queue.get(timeout=1)
            if city == "DONE":
                logging.info(f"{threading.current_thread().name}: DONE signal received, exiting")
                break

            url = f'{TOP_API_URL}/record/{city}/{recno}'
            logging.info(f"{threading.current_thread().name}: Fetching data from {url}")
            data = get_data_from_server(url)

            if data:
                logging.info(f"{threading.current_thread().name}: Data received: {data}")
                data_queue.put((city, data['date'], data['temp']))
            else:
                logging.warning(f"{threading.current_thread().name}: Failed to retrieve data from {url}")

            commands_queue.task_done()  # signal that the task is done

        except queue.Empty:
            # If commands_queue is truly empty and no more items are expected, break.
            # Otherwise, continue to loop and try again.
            if commands_queue.empty() and threading.active_count() <= THREADS + WORKERS + 1: # check if main is only one left
                 # This condition is tricky. A more robust way is to just let DONE signals propagate.
                 # With proper DONE signaling, this block might not be strictly necessary for correct exit.
                 # However, it helps prevent indefinite waiting if DONE signals are somehow missed or delayed.
                logging.info(f"{threading.current_thread().name}: commands_queue seems empty, checking for exit conditions.")
                break
            continue # Important to continue to next iteration if queue not empty
        except Exception as e:
            logging.error(f"{threading.current_thread().name}: An error occurred: {e}", exc_info=True)  # Log with traceback
            break  # Exit thread on error

    logging.info(f"{threading.current_thread().name}: Thread finished")


# ---------------------------------------------------------------------------
class Worker(threading.Thread): # Reverted to threading.Thread
    def __init__(self, data_queue, noaa, lock): # Added lock for thread safety
        super().__init__()
        self.data_queue = data_queue
        self.noaa = noaa
        self.lock = lock # Pass the lock to the worker
        self.daemon = True  # Allow main thread to exit even if workers are blocked

    def run(self):
        while True:
            try:
                city, date, temp = self.data_queue.get(timeout=1)
                if city == "DONE":
                    logging.info(f"{threading.current_thread().name}: DONE signal received, exiting")
                    break

                # Store data using the NOAA class, protecting it with the lock
                with self.lock: # Acquire lock before modifying shared NOAA data
                    self.noaa.store_data(city, date, temp)
                logging.debug(f"{threading.current_thread().name}: Stored data: city={city}, date={date}, temp={temp}")
                self.data_queue.task_done()  # signal that the task is done

            except queue.Empty:
                # Same as above, checking for empty condition for robust exit
                if self.data_queue.empty() and threading.active_count() <= THREADS + WORKERS + 1:
                    logging.info(f"{threading.current_thread().name}: data_queue seems empty, checking for exit conditions.")
                    break
                continue
            except Exception as e:
                logging.error(f"{threading.current_thread().name}: An error occurred: {e}", exc_info=True)
                break  # Exit worker on error

        logging.info(f"{threading.current_thread().name}: Worker finished")


# ---------------------------------------------------------------------------
class NOAA:
    def __init__(self):
        self.city_data = {}
        # No lock needed here, it's managed by the caller (Worker class)

    def store_data(self, city, date, temp):
        # This method is called from Worker, which now handles the lock
        if city not in self.city_data:
            self.city_data[city] = []
        self.city_data[city].append((date, temp))

    def get_temp_details(self, city):
        if city not in self.city_data:
            return 0.0
        temps = [temp for date, temp in self.city_data[city]]
        if not temps: # Handle case where list might be empty
            return 0.0
        return sum(temps) / len(temps)


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):
    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        avg = noaa.get_temp_details(name) # NOAA's data is consistent after all workers finish
        answer = answers[name]

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():
    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()
    # Create a lock for thread-safe access to NOAA's city_data
    noaa_lock = threading.Lock()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')
    if not data:
        logging.critical("Failed to start server, exiting.")
        return

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    print(f'{"City":>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        if city_details[name]:
            print(f'{name:>15}: Records = {city_details[name]["records"]:,}')
        else:
            print(f'{name:>15}: Failed to retrieve city details')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # Create queues
    commands_queue = queue.Queue(maxsize=10) # Reverted to queue.Queue
    data_queue = queue.Queue(maxsize=10)     # Reverted to queue.Queue

    # Populate the commands queue
    for city in CITIES:
        for i in range(records):
            commands_queue.put((city, i))
    logging.info(f"commands_queue populated with {len(CITIES) * records} items")

    # Create data retrieval threads
    threads = []
    for i in range(THREADS):
        thread = threading.Thread(target=retrieve_weather_data, args=(commands_queue, data_queue), name=f"Retriever-{i}")
        threads.append(thread)
        thread.start()
    logging.info(f"{THREADS} data retrieval threads started")

    # Create worker threads
    workers = []
    for i in range(WORKERS):
        # Pass the NOAA object and the lock to the Worker threads
        worker = Worker(data_queue, noaa, noaa_lock) 
        workers.append(worker)
        worker.start()
    logging.info(f"{WORKERS} worker threads started")

    # Add "all done" messages to the commands queue
    # This must be done AFTER all commands are put into the queue
    for _ in range(THREADS):
        commands_queue.put(("DONE", None))
    logging.info(f"{THREADS} DONE signals added to commands_queue")

    # Wait for all tasks in commands_queue to be processed
    commands_queue.join()
    logging.info("commands_queue joined")

    # Add "all done" messages to the data queue (only after commands_queue is done)
    # This ensures all data has been put into data_queue before signalling workers to stop
    for _ in range(WORKERS):
        data_queue.put(("DONE", None, None))
    logging.info(f"{WORKERS} DONE signals added to data_queue")
    
    # Wait for all tasks in data_queue to be processed
    data_queue.join()
    logging.info("data_queue joined")


    # Wait for all threads to finish (with timeout for robustness)
    for thread in threads:
        thread.join(timeout=5)
        if thread.is_alive():
            logging.warning(f"Thread {thread.name} did not terminate properly")
    logging.info("Data retrieval threads joined")

    for worker in workers:
        worker.join(timeout=5)
        if worker.is_alive():
            logging.warning(f"Worker {worker.name} did not terminate properly")
    logging.info("Worker threads joined")


    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    if data:
        print(data)
    else:
        print("Failed to get /end data")

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()