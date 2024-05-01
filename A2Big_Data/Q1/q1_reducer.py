#!/usr/bin/env python3
import sys
from collections import defaultdict

# Setup dictionaries for cumulative volumes and tally of entries
total_volumes = defaultdict(int)
total_counts = defaultdict(int)

for record in sys.stdin:
    # Decompose the input record into parts
    parts = record.strip().split(',')

    # Assign and transform parts to respective data types
    data_year = int(parts[0])
    data_key = parts[1]
    data_volume = float(parts[2])
    data_count = int(parts[3])  # Process occurrences count
    
    # Utilize a tuple (data_year, data_key) as the dictionary key
    record_key = (data_year, data_key)  

    # Update the total volume and count for each key
    total_volumes[record_key] += data_volume
    total_counts[record_key] += data_count

# Order the dictionary keys (data_year, data_key) by year
ordered_keys = sorted(total_volumes.keys(), key=lambda item: item[0])

# Output the average volume per (year, key) tuple
for (year, key) in ordered_keys:
    avg_volume = total_volumes[(year, key)] / total_counts[(year, key)]
    avg_volume = round(avg_volume, 0) if avg_volume.is_integer() else round(avg_volume, 2)
    print(f'{year},{key},{avg_volume}')
