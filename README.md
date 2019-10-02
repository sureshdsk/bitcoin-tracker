
# Bitcoin Tracker

## Requirements

* Kafka
* Redis
* Python 3
* [Python Virtual Env](https://virtualenv.pypa.io/en/latest/installation/)


## Create Kafka Topics

`
path to kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic btctxcounter
`

`
path to kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic btctxaggregator
`

`
path to kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic btctxdetails
`

## Config
All components share same configuration file which is at the root of this repository.

## Producer
Open a new terminal tab

cd into /producer directory and create a virtual environment

`
virtualenv -p python3 venv
`

run the producer using the command below

`
./run.sh
`


## Consumer
Open a new terminal tab

cd into /consumer directory and create a virtual environment

`
virtualenv -p python3 venv
`

run the consumer using the command below

`
./run.sh
`


## API
Open a new terminal tab

cd into /api directory and create a virtual environment

`
virtualenv -p python3 venv
`

run the flask api using the command below

`
./run.sh
`