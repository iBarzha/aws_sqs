import boto3
import json
import time
from botocore.exceptions import ClientError
from typing import List, Dict, Optional


class LocalStackSQSManager:
    def __init__(self, endpoint_url: str = "http://localhost:4566", region: str = "us-east-1"):
        """
        Initialize SQS client for LocalStack

        Args:
            endpoint_url: LocalStack URL (default localhost:4566)
            region: AWS region (can be any for LocalStack)
        """
        self.sqs = boto3.client(
            'sqs',
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )
        self.region = region

    def create_queue(self, queue_name: str, attributes: Optional[Dict] = None) -> str:
        try:
            if attributes is None:
                attributes = {
                    'VisibilityTimeoutSeconds': '60',
                    'MessageRetentionPeriod': '1209600',
                    'ReceiveMessageWaitTimeSeconds': '20',
                    'DelaySeconds': '0'
                }

            response = self.sqs.create_queue(
                QueueName=queue_name,
                Attributes=attributes
            )

            queue_url = response['QueueUrl']
            print(f" Queue created: {queue_name}")
            print(f"   URL: {queue_url}")
            return queue_url

        except ClientError as e:
            print(f" Queue creation error: {e}")
            return None

    def create_fifo_queue(self, queue_name: str) -> str:
        """
        Create a FIFO queue (First In, First Out)
        FIFO queues guarantee order and message uniqueness
        """
        fifo_name = f"{queue_name}.fifo"

        attributes = {
            'FifoQueue': 'true',
            'ContentBasedDeduplication': 'true',
            'VisibilityTimeoutSeconds': '60',
            'MessageRetentionPeriod': '1209600'
        }

        try:
            response = self.sqs.create_queue(
                QueueName=fifo_name,
                Attributes=attributes
            )

            queue_url = response['QueueUrl']
            print(f" FIFO queue created: {fifo_name}")
            return queue_url

        except ClientError as e:
            print(f" FIFO queue creation error: {e}")
            return None

    def create_dead_letter_queue(self, main_queue_name: str, dlq_name: str, max_receive_count: int = 3) -> tuple:
        """
        Create main queue with Dead Letter Queue
        DLQ is used for messages that cannot be processed

        Returns:
            Tuple of (main_queue_url, dlq_url)
        """
        dlq_url = self.create_queue(dlq_name)
        if not dlq_url:
            return None, None

        dlq_attributes = self.sqs.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=['QueueArn']
        )
        dlq_arn = dlq_attributes['Attributes']['QueueArn']

        redrive_policy = {
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": max_receive_count
        }

        main_attributes = {
            'VisibilityTimeoutSeconds': '60',
            'RedrivePolicy': json.dumps(redrive_policy)
        }

        main_queue_url = self.create_queue(main_queue_name, main_attributes)

        print(f"DLQ configured: {dlq_name} for queue {main_queue_name}")
        print(f"Max attempts: {max_receive_count}")

        return main_queue_url, dlq_url

    def list_queues(self, prefix: str = None) -> List[str]:
        """
        Get list of all queues

        Args:
            prefix: Filter by queue name prefix

        Returns:
            List of queue URLs
        """
        try:
            if prefix:
                response = self.sqs.list_queues(QueueNamePrefix=prefix)
            else:
                response = self.sqs.list_queues()

            queues = response.get('QueueUrls', [])

            print(f"Found queues: {len(queues)}")
            for i, queue_url in enumerate(queues, 1):
                queue_name = queue_url.split('/')[-1]
                print(f"   {i}. {queue_name}")

            return queues

        except ClientError as e:
            print(f"Error getting queue list: {e}")
            return []

    def send_message(self, queue_url: str, message_body: str, attributes: Optional[Dict] = None,
                     delay_seconds: int = 0) -> bool:
        try:
            params = {
                'QueueUrl': queue_url,
                'MessageBody': message_body
            }

            if delay_seconds > 0:
                params['DelaySeconds'] = delay_seconds

            if attributes:
                message_attributes = {}
                for key, value in attributes.items():
                    if isinstance(value, str):
                        message_attributes[key] = {
                            'StringValue': value,
                            'DataType': 'String'
                        }
                    elif isinstance(value, (int, float)):
                        message_attributes[key] = {
                            'StringValue': str(value),
                            'DataType': 'Number'
                        }

                params['MessageAttributes'] = message_attributes

            response = self.sqs.send_message(**params)

            message_id = response['MessageId']
            print(f"Message sent")
            print(f"ID: {message_id}")
            print(f"Body: {message_body[:50]}...")
            if delay_seconds > 0:
                print(f"   Delay: {delay_seconds} sec")

            return True

        except ClientError as e:
            print(f"Message sending error: {e}")
            return False

    def send_fifo_message(self, queue_url: str, message_body: str, group_id: str, deduplication_id: str = None) -> bool:
        """
        Send message to FIFO queue

        Args:
            queue_url: FIFO queue URL
            message_body: Message text
            group_id: Message group ID (for sorting)
            deduplication_id: Deduplication ID (if not using ContentBasedDeduplication)
        """
        try:
            params = {
                'QueueUrl': queue_url,
                'MessageBody': message_body,
                'MessageGroupId': group_id
            }

            if deduplication_id:
                params['MessageDeduplicationId'] = deduplication_id

            response = self.sqs.send_message(**params)

            print(f"FIFO message sent")
            print(f"ID: {response['MessageId']}")
            print(f"Group: {group_id}")

            return True

        except ClientError as e:
            print(f"FIFO message sending error: {e}")
            return False

    def send_batch_messages(self, queue_url: str, messages: List[Dict]) -> bool:
        """
        Send batch of messages (up to 10 at once)
        More efficient than sending one by one

        Args:
            queue_url: Queue URL
            messages: List of message dictionaries
                     [{'Id': 'msg1', 'MessageBody': 'text'}, ...]
        """
        try:
            batch_size = 10

            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]

                response = self.sqs.send_message_batch(
                    QueueUrl=queue_url,
                    Entries=batch
                )

                successful = len(response.get('Successful', []))
                failed = len(response.get('Failed', []))

                print(f"Batch sent: {successful} successful, {failed} errors")

                if failed > 0:
                    for failure in response['Failed']:
                        print(f"Error for {failure['Id']}: {failure['Message']}")

            return True

        except ClientError as e:
            print(f"Batch sending error: {e}")
            return False

    def receive_messages(self, queue_url: str, max_messages: int = 1, wait_time: int = 20,
                         visibility_timeout: int = 30) -> List[Dict]:
        """
        Receive messages from queue

        Args:
            queue_url: Queue URL
            max_messages: Maximum messages at once (1-10)
            wait_time: Long Polling wait time (0-20 sec)
            visibility_timeout: Message visibility timeout (0-43200 sec)

        Returns:
            List of received messages
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=wait_time,
                VisibilityTimeoutSeconds=visibility_timeout,
                MessageAttributeNames=['All'],
                AttributeNames=['All']
            )

            messages = response.get('Messages', [])

            if messages:
                print(f"Messages received: {len(messages)}")
                for i, message in enumerate(messages, 1):
                    print(f"{i}. ID: {message['MessageId']}")
                    print(f"Body: {message['Body'][:100]}...")
                    print(f"Receipt Handle: {message['ReceiptHandle'][:50]}...")

                    if 'MessageAttributes' in message:
                        print(f"      Attributes: {message['MessageAttributes']}")
            else:
                print("No messages")

            return messages

        except ClientError as e:
            print(f"Message receiving error: {e}")
            return []

    def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        """
        Delete message from queue
        Required after successful processing!

        Args:
            queue_url: Queue URL
            receipt_handle: Handle for deletion (from receive_message)
        """
        try:
            self.sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )

            print(f"Message deleted from queue")
            return True

        except ClientError as e:
            print(f"Message deletion error: {e}")
            return False

    def delete_batch_messages(self, queue_url: str, messages: List[Dict]) -> bool:
        """
        Batch delete messages

        Args:
            queue_url: Queue URL
            messages: List of messages with ReceiptHandle
        """
        try:
            entries = []
            for i, message in enumerate(messages):
                entries.append({
                    'Id': str(i),
                    'ReceiptHandle': message['ReceiptHandle']
                })

            response = self.sqs.delete_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )

            successful = len(response.get('Successful', []))
            failed = len(response.get('Failed', []))

            print(f" Batch deletion: {successful} successful, {failed} errors")

            return failed == 0

        except ClientError as e:
            print(f"Batch deletion error: {e}")
            return False

    def change_message_visibility(self, queue_url: str, receipt_handle: str, visibility_timeout: int) -> bool:
        """
        Change message visibility timeout
        Useful for extending processing time

        Args:
            queue_url: Queue URL
            receipt_handle: Message handle
            visibility_timeout: New visibility timeout in seconds
        """
        try:
            self.sqs.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout
            )

            print(f"Visibility timeout changed to {visibility_timeout} sec")
            return True

        except ClientError as e:
            print(f"Visibility timeout change error: {e}")
            return False

    def get_queue_attributes(self, queue_url: str) -> Dict:
        """
        Get queue attributes and statistics

        Returns:
            Dictionary with queue attributes
        """
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )

            attributes = response['Attributes']

            print(f"Queue attributes:")
            print(f"Visible messages: {attributes.get('ApproximateNumberOfMessages', 0)}")
            print(f"In flight: {attributes.get('ApproximateNumberOfMessagesNotVisible', 0)}")
            print(f"Delayed: {attributes.get('ApproximateNumberOfMessagesDelayed', 0)}")
            print(f"Visibility timeout: {attributes.get('VisibilityTimeoutSeconds', 0)} sec")
            print(f"Retention period: {attributes.get('MessageRetentionPeriod', 0)} sec")

            return attributes

        except ClientError as e:
            print(f"Error getting attributes: {e}")
            return {}

    def purge_queue(self, queue_url: str) -> bool:
        """
        Clear all messages from queue
        """
        try:
            self.sqs.purge_queue(QueueUrl=queue_url)
            print(f"Queue purged of all messages")
            return True

        except ClientError as e:
            print(f"Queue purging error: {e}")
            return False

    def delete_queue(self, queue_url: str) -> bool:
        """
        Complete queue deletion
        """
        try:
            self.sqs.delete_queue(QueueUrl=queue_url)
            queue_name = queue_url.split('/')[-1]
            print(f"Queue deleted: {queue_name}")
            return True

        except ClientError as e:
            print(f"Queue deletion error: {e}")
            return False


def demonstrate_sqs_operations():
    """
    Demonstration of all SQS capabilities
    Complete example of using all methods
    """
    print("Starting AWS SQS demonstration with LocalStack\n")

    # Initialize SQS manager
    sqs_manager = LocalStackSQSManager()

    # 1. Creating different types of queues
    print("=" * 60)
    print("CREATING QUEUES")
    print("=" * 60)

    standard_queue = sqs_manager.create_queue("demo-standard-queue")

    fifo_queue = sqs_manager.create_fifo_queue("demo-fifo-queue")

    main_queue, dlq = sqs_manager.create_dead_letter_queue(
        "demo-main-queue",
        "demo-dlq-queue",
        max_receive_count=2
    )

    time.sleep(1)

    print("\n" + "=" * 60)
    print("QUEUE LIST")
    print("=" * 60)

    all_queues = sqs_manager.list_queues()

    print("\n" + "=" * 60)
    print("3SENDING MESSAGES")
    print("=" * 60)

    if standard_queue:
        sqs_manager.send_message(
            standard_queue,
            "Hello from standard queue!",
            attributes={
                "Author": "Demo Script",
                "Priority": 1,
                "Category": "Test"
            }
        )

        sqs_manager.send_message(
            standard_queue,
            "Delayed message",
            delay_seconds=5
        )

    if fifo_queue:
        for i in range(3):
            sqs_manager.send_fifo_message(
                fifo_queue,
                f"FIFO message #{i + 1}",
                group_id="demo-group",
                deduplication_id=f"msg-{i + 1}"
            )

    if main_queue:
        batch_messages = []
        for i in range(5):
            batch_messages.append({
                'Id': f'batch-msg-{i}',
                'MessageBody': f'Batch message #{i + 1}',
                'MessageAttributes': {
                    'BatchNumber': {
                        'StringValue': str(i),
                        'DataType': 'Number'
                    }
                }
            })

        sqs_manager.send_batch_messages(main_queue, batch_messages)

    print("\n" + "=" * 60)
    print("RECEIVING MESSAGES")
    print("=" * 60)

    if standard_queue:
        print("\nReceiving from standard queue:")
        messages = sqs_manager.receive_messages(standard_queue, max_messages=3, wait_time=5)

        for message in messages:
            print(f"Processing message: {message['Body']}")

            time.sleep(1)

            sqs_manager.delete_message(standard_queue, message['ReceiptHandle'])

    if fifo_queue:
        print("\nReceiving from FIFO queue:")
        fifo_messages = sqs_manager.receive_messages(fifo_queue, max_messages=5, wait_time=2)

        if fifo_messages:
            print("🗑Batch deleting FIFO messages:")
            sqs_manager.delete_batch_messages(fifo_queue, fifo_messages)

    print("\n" + "=" * 60)
    print("QUEUE STATISTICS")
    print("=" * 60)

    for queue_url in [standard_queue, fifo_queue, main_queue, dlq]:
        if queue_url:
            queue_name = queue_url.split('/')[-1]
            print(f"\nStatistics for {queue_name}:")
            sqs_manager.get_queue_attributes(queue_url)

    print("\n" + "=" * 60)
    print("VISIBILITY MANAGEMENT")
    print("=" * 60)

    if main_queue:
        sqs_manager.send_message(main_queue, "Message for visibility test")

        messages = sqs_manager.receive_messages(main_queue, max_messages=1, wait_time=2)

        if messages:
            message = messages[0]
            print("Extending processing time...")

            # Extend visibility timeout
            sqs_manager.change_message_visibility(
                main_queue,
                message['ReceiptHandle'],
                visibility_timeout=120
            )

            sqs_manager.delete_message(main_queue, message['ReceiptHandle'])

    print("\n" + "=" * 60)
    print("DEAD LETTER QUEUE DEMO")
    print("=" * 60)

    if main_queue and dlq:
        sqs_manager.send_message(main_queue, "Problem message for DLQ")

        print("Simulating failed processing...")

        for attempt in range(3):
            messages = sqs_manager.receive_messages(main_queue, max_messages=1, wait_time=1)
            if messages:
                print(f"   Processing attempt #{attempt + 1} - 'failed'")
                time.sleep(2)

        time.sleep(3)
        print("\nChecking Dead Letter Queue:")
        dlq_messages = sqs_manager.receive_messages(dlq, max_messages=1, wait_time=2)

        if dlq_messages:
            print("Message arrived in DLQ as expected")
            sqs_manager.delete_batch_messages(dlq, dlq_messages)
        else:
            print("No messages in DLQ yet (may take some time)")

    # 8. Cleanup (optional)
    print("\n" + "=" * 60)
    print("CLEANUP")
    print("=" * 60)

    cleanup = input("\n🧹 Do you want to clean up created queues? (y/N): ").lower()

    if cleanup == 'y':
        for queue_url in [standard_queue, fifo_queue, main_queue, dlq]:
            if queue_url:
                sqs_manager.purge_queue(queue_url)
                time.sleep(1)

                sqs_manager.delete_queue(queue_url)

        print("Cleanup completed!")
    else:
        print("Queues left for further experiments")

    print("\nDemonstration completed!")
    print("You've learned all main AWS SQS operations!")


if __name__ == "__main__":
    demonstrate_sqs_operations()