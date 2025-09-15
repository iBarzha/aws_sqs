import boto3
import time
from datetime import datetime


def monitor_queues():
    """SQS queues monitoring"""
    sqs = boto3.client(
        'sqs',
        endpoint_url='http://localhost:4566',
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )

    try:
        while True:
            print(f"\n{'=' * 80}")
            print(f"Monitoring SQS queues - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'=' * 80}")

            response = sqs.list_queues()
            queues = response.get('QueueUrls', [])

            if not queues:
                print("The queues are not found")
                time.sleep(10)
                continue

            for queue_url in queues:
                queue_name = queue_url.split('/')[-1]

                attrs = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['All']
                )['Attributes']

                visible = int(attrs.get('ApproximateNumberOfMessages', 0))
                in_flight = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
                delayed = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))

                print(f"\n {queue_name}")
                print(f"Visible messages: {visible}")
                print(f"In flight: {in_flight}")
                print(f"Delayed: {delayed}")
                print(f"All in line: {visible + in_flight + delayed}")

                total_messages = visible + in_flight + delayed
                if total_messages == 0:
                    status = "Empty"
                elif total_messages < 10:
                    status = "NORMAL"
                elif total_messages < 100:
                    status = "Loaded"
                else:
                    status = "Crowded"

                print(f"   📈 Статус: {status}")

            time.sleep(10)

    except KeyboardInterrupt:
        print("\n⏹Monitoring is stopped")


if __name__ == "__main__":
    monitor_queues()