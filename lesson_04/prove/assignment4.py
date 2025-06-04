"""
Course    : CSE 351
Assignment: 04
Student   : Kian LaMer

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
import threading
import queue
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
                logging.info("DONE signal received, exiting")
                break

            url = f'{TOP_API_URL}/record/{city}/{recno}'
            logging.info(f"Fetching data from {url}")
            data = get_data_from_server(url)

            if data:
                logging.info(f"Data received: {data}")
                data_queue.put((city, data['date'], data['temp']))
            else:
                logging.warning(f"Failed to retrieve data from {url}")

            commands_queue.task_done()  # signal that the task is done

        except queue.Empty:
            if commands_queue.empty():
                logging.info("commands_queue is empty, exiting")
                break
            continue  # Important to continue to next iteration if queue not empty
        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)  # Log with traceback
            break  # Exit thread on error

    logging.info("Thread finished")


# ---------------------------------------------------------------------------
class Worker(threading.Thread):
    def __init__(self, data_queue, noaa):
        super().__init__()
        self.data_queue = data_queue
        self.noaa = noaa
        self.daemon = True  # Allow main thread to exit even if workers are blocked

    def run(self):
        while True:
            try:
                city, date, temp = self.data_queue.get(timeout=1)
                if city == "DONE":
                    logging.info("DONE signal received, exiting")
                    break

                self.noaa.store_data(city, date, temp)
                logging.debug(f"Stored data: city={city}, date={date}, temp={temp}")
                self.data_queue.task_done()  # signal that the task is done

            except queue.Empty:
                if self.data_queue.empty():
                    logging.info("data_queue is empty, exiting")
                    break
                continue  # Important to continue to next iteration if queue not empty
            except Exception as e:
                logging.error(f"An error occurred: {e}", exc_info=True)
                break  # Exit worker on error

        logging.info("Worker finished")


# ---------------------------------------------------------------------------
class NOAA:
    def __init__(self):
        self.city_data = {}

    def store_data(self, city, date, temp):
        if city not in self.city_data:
            self.city_data[city] = []
        self.city_data[city].append((date, temp))

    def get_temp_details(self, city):
        if city not in self.city_data:
            return 0.0
        temps = [temp for date, temp in self.city_data[city]]
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
        answer = answers[name]
        avg = noaa.get_temp_details(name)

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

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')
    if not data:
        logging.critical("Failed to start server, exiting.")
        return

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
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
    commands_queue = queue.Queue(maxsize=10)
    data_queue = queue.Queue(maxsize=10)

    # Populate the commands queue
    for city in CITIES:
        for i in range(records):
            commands_queue.put((city, i))
    logging.info(f"commands_queue populated with {len(CITIES) * records} items")

    # Create threads
    threads = []
    for i in range(THREADS):
        thread = threading.Thread(target=retrieve_weather_data, args=(commands_queue, data_queue))
        threads.append(thread)
        thread.start()
    logging.info(f"{THREADS} data retrieval threads started")

    # Create workers
    workers = []
    for i in range(WORKERS):
        worker = Worker(data_queue, noaa)
        workers.append(worker)
        worker.start()
    logging.info(f"{WORKERS} worker threads started")

    # Add "all done" messages to the commands queue
    for _ in range(THREADS):
        commands_queue.put(("DONE", None))
    logging.info(f"{THREADS} DONE signals added to commands_queue")

    # Add "all done" messages to the data queue
    for _ in range(WORKERS):
        data_queue.put(("DONE", None, None))
    logging.info(f"{WORKERS} DONE signals added to data_queue")

    # Wait for all tasks to be done
    commands_queue.join()
    data_queue.join()
    logging.info("Both queues joined")

    # Wait for all threads to finish (with timeout)
    for thread in threads:
        thread.join(timeout=5)  # Add timeout to prevent indefinite wait
        if thread.is_alive():
            logging.warning(f"Thread {thread.name} did not terminate properly")
    logging.info("Data retrieval threads joined")

    # Wait for all workers to finish (with timeout)
    for worker in workers:
        worker.join(timeout=5)  # Add timeout
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