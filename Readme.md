# AWS SQS: Best Practices and Demo

This guide covers essential aspects of working with **AWS SQS** using **LocalStack**, including setup and message operations

### Running the Demo

This section provides instructions for running the demonstration code.

#### 1. Start LocalStack

First, start the LocalStack container to simulate the AWS environment locally.

docker-compose up -d
2. Install Dependencies
Install the necessary Python libraries, including boto3, to interact with SQS.

pip install boto3
3. Run the Demo
Execute the main demonstration script.

python sql_operations.py
4. Individual Components
You can also run the components separately in different terminal windows:

# Start the producer in one terminal
python producer.py

# Start the consumer in another terminal
python consumer.py

# Start the monitor in a third terminal
python monitor.py
Stopping and Cleanup
To stop the services and clean up your environment, use the following commands.

# Stop LocalStack
docker-compose down

# Full cleanup (including volumes)
docker-compose down -v

# Delete LocalStack images
docker rmi localstack/localstack:latest


# Overview
This code provides a comprehensive overview of working with AWS SQS using LocalStack. It covers:

Local environment setup

Queue types (Standard, FIFO, DLQ)

Message operations

Producer/Consumer patterns

Monitoring 