#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import csv
from multiprocessing import Pool, Manager
import sys

def read_csv(filepath, start, end):
    """Read a CSV file and return a slice from start to end lines."""
    data = []
    with open(filepath, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header
        for i, row in enumerate(csv_reader):
            if i >= end:
                break
            if i >= start:
                data.append(row)
    return data

def map_function(data):
    """Extract artist and duration, emit as (artist, duration)."""
    result = {}
    for row in data:
        try:
            artist = row[2]  # Assuming artist name is in column 3
            duration = float(row[3])  # Assuming duration is in column 4
            if artist in result:
                result[artist] = max(result[artist], duration)
            else:
                result[artist] = duration
        except ValueError:
            continue  # Skip rows with invalid data
    return result

def shuffle_function(mapped_data):
    """Shuffle data to combine all results by key."""
    shuffled = {}
    for data_part in mapped_data:
        for key, value in data_part.items():
            if key in shuffled:
                shuffled[key].append(value)
            else:
                shuffled[key] = [value]
    return shuffled

def reduce_function(data):
    """Reduce function to find the maximum duration for each artist."""
    result = {key: max(values) for key, values in data.items()}
    return result

def main(filepath, num_mappers, num_reducers):
    # Determine the size of each chunk of the CSV to read
    with open(filepath, 'r') as file:
        num_lines = sum(1 for line in file) - 1  # subtract 1 to exclude header

    chunk_size = num_lines // num_mappers
    pool = Pool(processes=num_mappers)
    manager = Manager()
    
    # Map phase
    mapped = pool.starmap(map_function, [(read_csv(filepath, i * chunk_size, (i + 1) * chunk_size),) for i in range(num_mappers)])
    
    # Shuffle phase
    shuffled = shuffle_function(mapped)
    
    # Reduce phase (could be parallelized by number of reducers, here simplified)
    reduced = reduce_function(shuffled)

    # Output results
    for artist, max_duration in reduced.items():
        print(f'{artist}: {max_duration}')

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python songs.py <file_path> <num_mappers> <num_reducers>")
        sys.exit(1)
    file_path = sys.argv[1]
    num_mappers = int(sys.argv[2])
    num_reducers = int(sys.argv[3])
    main(file_path, num_mappers, num_reducers)


