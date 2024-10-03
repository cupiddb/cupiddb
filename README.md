# CupidDB
CupidDB is a high-performance in-memory database designed for efficient storage and retrieval of byte-level data. Built to handle columnar data in the Apache Arrow format, it enables rapid data manipulation and minimizes network overhead, making it ideal for caching DataFrames.

## Client Library
* [`pycupiddb`]: Python client for CupidDB

[`pycupiddb`]: https://github.com/cupiddb/pycupiddb


## Production Build
```
cargo build --release
```

## Production Docker Build
```
docker build -t cupiddb:latest --target runner .
```

## Environment Variables
| Variable Name          | Description                                                                                                            | Possible Values                 | Default Value                 |
|------------------------|------------------------------------------------------------------------------------------------------------------------|---------------------------------|-------------------------------|
| CUPID_LOG_LEVEL        | Log level                                                                                                              | ERROR, WARN, INFO, DEBUG, TRACE | INFO                          |
| CUPID_WORKER_THREADS   | Number of worker threads CupidDB will use. The recommended value is the number of CPU cores.                           | Positive integer                | Number of CPU cores available |
| CUPID_CACHE_SHARDS     | Number of separate buckets, each with its own lock, allowing multiple threads to access different shards concurrently. | 2^n                             | 64                            |
| CUPID_INITIAL_CAPACITY | Number of key-value pairs the map can hold before needing to resize                                                    | Positive integer                | 64                            |
| CUPID_GRACEFUL_TIMEOUT | Number of seconds CupidDB will wait for client's command to complete before completely shutting down                   | Positive integer                | 30                            |
| CUPID_BIND_ADDRESS     | The address CupidDB will bind to                                                                                       | IP address                      | 0.0.0.0                       |
| CUPID_PORT             | The port number CupidDB will listen to                                                                                 |                                 | 5995                          |
