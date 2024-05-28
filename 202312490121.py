import csv
from collections import defaultdict
from threading import Thread


# map passenger flights
def map_passenger_flights(file_path):
    passenger_flight_counts = defaultdict(int)  # Dictionary:count flights per passenger
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            passenger_id = row[0]  # get passenger ID
            passenger_flight_counts[passenger_id] += 1  # increment flight count
    return passenger_flight_counts


# reduce map results
def reduce_passenger_flights(map_results):
    combined_counts = defaultdict(int)
    for partial_result in map_results:
        for passenger_id, count in partial_result.items():
            combined_counts[passenger_id] += count  # combine counts
    return combined_counts


# find max flights
def find_max_flights(passenger_flight_counts):
    max_flights = max(passenger_flight_counts.values())  # maximum flight count
    top_passengers = [pid for pid, count in passenger_flight_counts.items() if
                      count == max_flights]  # Find top passengers
    return top_passengers, max_flights  # Return:top passengers , max flights


# parallel map
def parallel_map(file_path, num_threads=4):
    size = sum(1 for _ in open(file_path))  # get total lines
    chunk_size = size // num_threads  # calculate chunk size
    map_results = [None] * num_threads

    def map_worker(file_path, start_line, num_lines, index):
        passenger_flight_counts = defaultdict(int)
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            for i, row in enumerate(reader):
                if i < start_line:  # skip lines before start_line
                    continue
                if i >= start_line + num_lines:  # stop after num_lines
                    break
                passenger_id = row[0]  # get passenger ID
                passenger_flight_counts[passenger_id] += 1  # Increment flight count
        map_results[index] = passenger_flight_counts  # Store result into map_result

    threads = []  # threads
    for i in range(num_threads):
        start_line = i * chunk_size  # Calculate start line
        num_lines = chunk_size if i < num_threads - 1 else size - start_line  # calculate the number of lines
        thread = Thread(target=map_worker, args=(file_path, start_line, num_lines, i))  # create thread
        threads.append(thread)  # add thread to list
        thread.start()

    for thread in threads:  # wait for all threads to finish
        thread.join()

    return map_results



if __name__ == '__main__':
    file_path = 'AComp_Passenger_data_no_error.csv'  # Path to the data file

    # Step 1: Map phase in parallel
    map_results = parallel_map(file_path)

    # Step 2: Reduce phase
    passenger_flight_counts = reduce_passenger_flights(map_results)

    # Step 3: Find the passenger with the highest number of flights
    top_passengers, max_flights = find_max_flights(passenger_flight_counts)

    print(f'Top passenger(s): {top_passengers}')
    print(f'Maximum number of flights: {max_flights}')
