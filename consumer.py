import boto3
import json
import time
from typing import Dict, Any


class OrderProcessor:
    """Order processor from SQS queue"""

    def __init__(self):
        self.sqs = boto3.client(
            'sqs',
            endpoint_url='http://localhost:4566',
            region_name='us-east-1',
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )
        self.queue_url = None
        self.running = True

    def connect_to_queue(self, queue_name: str):
        """Connect to queue"""
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            self.queue_url = response['QueueUrl']
            print(f"Connected to queue: {queue_name}")
            return True
        except Exception as e:
            print(f"Queue connection error: {e}")
            return False

    def process_order(self, order_data: Dict[str, Any]) -> bool:
        """Process single order"""
        try:
            order_id = order_data.get('order_id')
            product = order_data.get('product')
            quantity = order_data.get('quantity')
            price = order_data.get('price')

            print(f"Processing order {order_id}:")
            print(f"Product: {product}")
            print(f"Quantity: {quantity}")
            print(f"Price: ${price}")

            processing_time = 2 if price > 500 else 1
            time.sleep(processing_time)

            import random
            if random.random() < 0.05:
                raise Exception("Order processing error")

            print(f"Order {order_id} processed successfully")
            return True

        except Exception as e:
            print(f"Order processing error: {e}")
            return False

    def start_processing(self):
        """Start message processing"""
        if not self.queue_url:
            print("Not connected to queue")
            return

        print("Starting message processing...")
        print("(Press Ctrl+C to stop)")

        try:
            while self.running:
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeoutSeconds=60,
                    MessageAttributeNames=['All'],
                    AttributeNames=['All']
                )

                messages = response.get('Messages', [])

                if not messages:
                    print("No new messages, waiting...")
                    continue

                print(f"Messages received: {len(messages)}")

                for message in messages:
                    try:
                        order_data = json.loads(message['Body'])

                        attributes = message.get('MessageAttributes', {})
                        priority = attributes.get('Priority', {}).get('StringValue', 'Normal')

                        print(f"\nNew order (priority: {priority})")

                        if self.process_order(order_data):
                            self.sqs.delete_message(
                                QueueUrl=self.queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            print("Message deleted from queue")
                        else:
                            print("Message left in queue for retry")

                    except json.JSONDecodeError:
                        print(f"The wrong message format: {message['Body']}")
                        self.sqs.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )

                    except Exception as e:
                        print(f"An unexpected mistake: {e}")

        except KeyboardInterrupt:
            print("\nA stop signal was obtained")
        finally:
            self.running = False
            print("The processing is stopped")


def main():
    processor = OrderProcessor()

    if processor.connect_to_queue('orders-queue'):
        processor.start_processing()


if __name__ == "__main__":
    main()