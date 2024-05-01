# Big_Data_A2

## Question 1
### Prompt 1
Can you convert this MapReduce javascript file I just uploaded into a py file without changing its functionality at all?

### Prompt 2
Please do the same for this reducer file


## Question 2
### Overview
This project implements a MapReduce approach to process a large dataset from the Million Song Database, specifically aiming to compute the maximum duration of songs for each artist. The dataset, totaling 42GB in size, consists of song metadata in CSV format with details such as song title, artist name, and song duration.

### Dataset
The dataset used in this project is a subset of the Million Song Dataset, which can be found at [Million Song Dataset Challenge on Kaggle](https://www.kaggle.com/c/msdchallenge#description). It includes the following columns:
- **Column 1**: Song title
- **Column 3**: Artist’s name
- **Column 4**: Song duration in seconds

### Methodology
The solution involves splitting the input CSV file into smaller chunks, processing these chunks in parallel using multiple mapper processes, and then using reducer processes to aggregate the results. This method allows for efficient data processing of large datasets on machines with multiple cores.

#### MapReduce Workflow
##### Map Phase
Each mapper processes a segment of the data, reading the artist's name and the duration, and emits intermediate key-value pairs where the key is the artist's name and the value is the duration.

##### Shuffle Phase
The shuffle operation collects all intermediate values associated with the same key (artist’s name) and groups them together for the reduce phase.

##### Reduce Phase
Each reducer receives grouped data by key and computes the maximum song duration for each artist.

### Execution
The script `songs.py` is executed with parameters specifying the number of map and reduce processes. For example, running the script as `python songs.py 20 5` would initiate 20 mapper processes and 5 reducer processes.




