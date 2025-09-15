import boto3
import json
from datetime import datetime


def create_producer():
    """Simple message producer"""
    sqs = boto3.client(
        'sqs',
        endpoint_url='http://localhost:4566',
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )

    queue_url = sqs.create_queue(QueueName='orders-queue')['QueueUrl']

    orders = [
        {'order_id': '001', 'product': 'Laptop', 'quantity': 1, 'price': 999.99},
        {'order_id': '002', 'product': 'Mouse', 'quantity': 2, 'price': 25.50},
        {'order_id': '003', 'product': 'Keyboard', 'quantity': 1, 'price': 79.99}
    ]

    for order in orders:
        message_body = json.dumps(order)

        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageAttributes={
                'OrderType': {
                    'StringValue': order['product'],
                    'DataType': 'String'
                },
                'Priority': {
                    'StringValue': 'High' if order['price'] > 500 else 'Normal',
                    'DataType': 'String'
                },
                'Timestamp': {
                    'StringValue': datetime.now().isoformat(),
                    'DataType': 'String'
                }
            }
        )

        print(f"Order sent: {order['order_id']} - {order['product']}")
        print(f"MessageId: {response['MessageId']}")


if __name__ == "__main__":
    create_producer()