
import sys
import re

def mapper():
    for line in sys.stdin:
        parts = line.strip().split()
        
        # Determine if the line is from 1-gram or 2-gram by checking the number of parts
        if len(parts) >= 4:
            if len(parts) == 4:  # 1-gram format: word year occurrences volumes
                word, year, occurrences, volumes = parts
                process_line(word, year, volumes, 1)
            elif len(parts) >= 5:  # 2-gram format: word1 word2 year occurrences volumes
                word1, word2, year, occurrences, volumes = parts[0], parts[1], parts[2], parts[3], parts[4]
                process_line(word1 + " " + word2, year, volumes, 2)

def process_line(words, year, volumes, gram_type):
    try:
        year = int(year)  # Filter out lines where year is not an integer
        volumes = int(volumes)
    except ValueError:
        return

    substrings = ['nu', 'chi', 'haw']
    word_list = words.split() if gram_type == 2 else [words]
    count_factors = sum(any(sub in word for sub in substrings) for word in word_list)
    if count_factors > 0:
        for sub in substrings:
            if any(sub in word for word in word_list):
                # Emit year, substring, total volumes (consider bi-gram words twice if needed)
                print(f"{year}\t{sub}\t{volumes * count_factors}")

def reducer():
    current_key = None
    total_volumes = 0
    count = 0
    
    for line in sys.stdin:
        year, substring, volumes = line.strip().split('\t')
        key = (year, substring)
        
        if current_key and key != current_key:
            print_result(current_key, total_volumes, count)
            total_volumes = 0
            count = 0
        
        total_volumes += int(volumes)
        count += 1
        current_key = key
    
    if current_key:
        print_result(current_key, total_volumes, count)

def print_result(key, total_volumes, count):
    year, substring = key
    average_volumes = total_volumes // count
    print(f"{year},{substring},{average_volumes}")

if __name__ == "__main__":
    if sys.argv[1] == 'mapper':
        mapper()
    elif sys.argv[1] == 'reducer':
        reducer()
