## Prerequisites

1. Docker and Docker Compose
2. Python 3.7 or later
3. `pip` and `virtualenv` for Python
4. `psql` (I use PostgreSQL-14.9)
5. Clone this repository:
   ```sh
   git clone git@github.com:risingwavelabs/risingwave-data-talks-workshop-2024-03-04.git
   cd risingwave-data-talks-workshop-2024-03-04
   ```
   Or, if you prefer HTTPS:
   ```sh
   git clone https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04.git
   cd risingwave-data-talks-workshop-2024-03-04
   ```

## Note on the dataset

The NYC Taxi dataset is a public dataset that contains information about taxi trips in New York City.
The dataset is available in Parquet format and can be downloaded from the [NYC Taxi & Limousine Commission website](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

We will be using the following files from the dataset:
- `yellow_tripdata_2022-01.parquet`
- `taxi_zone.csv`

For your convenience, these have already been downloaded and are available in the `data` directory.

The file `seed_kafka.py` contains the logic to process the data and populate RisingWave.

In this workshop, we will replace the `timestamp` fields in the `trip_data` with `timestamp`s close to the current time.
That's because `yellow_tripdata_2022-01.parquet` contains historical data from 2022,
and we want to simulate processing real-time data.

## Project Structure

```plaintext
$ tree -L 1
.
├── README.md                   # This file
├── clickhouse-sql              # SQL scripts for Clickhouse
├── commands.sh                 # Commands to operate the cluster
├── data                        # Data files (trip_data, taxi_zone)
├── docker                      # Contains docker compose files
├── requirements.txt            # Python dependencies
├── risingwave-sql              # SQL scripts for RisingWave (includes some homework files)
└── seed_kafka.py               # Python script to seed Kafka
```

## Getting Started

Before getting your hands dirty with the project, we will:
1. Run some diagnostics.
2. Start the RisingWave cluster.
3. Setup our python environment.

```bash
# Check version of psql
psql --version
source commands.sh

# Start the RW cluster
start-cluster

# Setup python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`commands.sh` contains several commands to operate the cluster. You may reference it to see what commands are available.
