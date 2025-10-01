# DB Connectors

A Python library providing unified connectors for various databases and message brokers.

## Features

- **Kafka Integration**: Both consumer and producer connectors using confluent-kafka and quixstreams
- **MinIO/S3 Support**: Object storage connector using boto3
- **TimescaleDB/PostgreSQL**: Database connector using SQLAlchemy
- **Unified Interface**: All connectors inherit from a common base class

## Installation

```bash
pip install db-connectors
```

## Quick Start

### Kafka Producer

```python
from db_connectors import KafkaProducerConnector

producer = KafkaProducerConnector(
    address="localhost",
    port=9092,
    target="my-topic"
)
producer.connect()
producer.produce_message("Hello, Kafka!")
producer.disconnect()
```

### MinIO Connector

```python
from db_connectors import MinIOConnector

minio = MinIOConnector(
    address="localhost",
    port=9000,
    target="my-bucket"
)
minio.connect()
minio.upload_object("file.txt", "Hello, MinIO!")
minio.disconnect()
```

### TimescaleDB Connector

```python
from db_connectors import TimescaleConnector

timescale = TimescaleConnector(
    address="localhost",
    port=5432,
    target="mydb"
)
timescale.connect()
# Use timescale.engine for SQLAlchemy operations
timescale.disconnect()
```

## Available Connectors

- `Connector` - Base abstract class
- `KafkaConnector` - Kafka consumer using quixstreams
- `KafkaProducerConnector` - Kafka producer using confluent-kafka
- `MinIOConnector` - MinIO/S3 object storage
- `TimescaleConnector` - TimescaleDB/PostgreSQL database

## Requirements

- Python >= 3.12
- confluent-kafka >= 2.3.0
- quixstreams >= 2.0.0
- boto3 >= 1.34.0
- sqlalchemy >= 2.0.0
- psycopg2-binary >= 2.9.0

## License

MIT License
