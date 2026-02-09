"""
VIZNX <BASE APP> SQS Lambda Consumer

AWS Lambda function handler for processing <BASE APP> requests from SQS.
"""

import json
import os
import logging
import time
import signal
import sys
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError


def process_sqs_message(message_body: str, cb_datamap: Dict[str, str]) -> None:
    """
    Process a single SQS message

    Example payload from SQS:
        {
            'data': {
                <DATA_KEY>: <DATA_VALUE>
            },
            'metadata': {
                'timestamp': '2025-10-06T19:48:45.380843',
                'source': '<SOURCE_NAME>',
                'version': '1.0',
                'host': 'sqs-host',
                'queue': '<QUEUE_NAME>'
            }
        }

    :param message_body: The SQS message body (JSON string)
    :param cb_datamap: A dict with the env data
    """
    # Load the message to a dict
    try:
        message_data = json.loads(message_body)
        logging.debug("[MESSAGE] %s", str(message_data))
    except Exception as ex:
        raise Exception(f"[Data Load Error] [{str(ex)}]") from ex

    # Extract the data field which contains the user information
    if 'data' not in message_data:
        raise ValueError("Message missing 'data' field")


# Setup a datamap for passing to callback
datamap = {
    "LOG_LEVEL": os.environ.get('LOG_LEVEL')
}

# set the logging output
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=getattr(logging, log_level),
                    format='%(levelname)s:%(name)s:%(asctime)s:%(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S')
logging.getLogger("botocore").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function for processing SQS events

    :param event: Lambda event containing SQS records
    :param context: Lambda context object
    :return: Response dictionary with status and results
    """
    logger = logging.getLogger(__name__)

    # Initialize response
    response = {
        'statusCode': 200,
        'processed': 0,
        'failed': 0,
        'errors': []
    }

    # Process each SQS record
    if 'Records' not in event:
        logger.warning("No 'Records' found in event")
        response['statusCode'] = 400
        response['errors'].append("No 'Records' found in event")
        return response

    for record in event['Records']:
        try:
            # Extract message body from SQS record
            if 'body' not in record:
                logger.error("Record missing 'body' field: %s", record)
                response['failed'] += 1
                response['errors'].append(f"Record missing 'body' field: {record.get('messageId', 'unknown')}")
                continue

            message_body = record['body']
            message_id = record.get('messageId', 'unknown')

            logger.info("Processing message: %s", message_id)

            # Process the message
            process_sqs_message(message_body, datamap)

            response['processed'] += 1
            logger.info("Successfully processed message: %s", message_id)

        except Exception as ex:
            logger.error("Error processing record: %s", str(ex), exc_info=True)
            response['failed'] += 1
            response['errors'].append(f"Error processing message {record.get('messageId', 'unknown')}: {str(ex)}")

    # Set status code based on results
    if response['failed'] > 0:
        response['statusCode'] = 207  # Multi-Status (partial success)
        if response['processed'] == 0:
            response['statusCode'] = 500  # All failed

    logger.info("Lambda execution complete: processed=%d, failed=%d",
                response['processed'], response['failed'])

    return response


def get_sqs_client(endpoint_url: Optional[str] = None, region_name: str = 'us-east-1',
                   access_key_id: str = 'test', secret_access_key: str = 'test'):
    """
    Create and return SQS client for LocalStack or AWS
    
    :param endpoint_url: LocalStack endpoint URL (e.g., http://localhost:4566)
    :param region_name: AWS region name
    :param access_key_id: AWS access key ID
    :param secret_access_key: AWS secret access key
    :return: SQS client
    """
    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key
    )

    if endpoint_url:
        # LocalStack or custom endpoint
        return session.client('sqs', region_name=region_name, endpoint_url=endpoint_url)

    # AWS SQS
    return session.client('sqs', region_name=region_name)


def get_queue_url(sqs_client, queue_name: str) -> Optional[str]:
    """
    Get queue URL from queue name
    
    :param sqs_client: SQS client
    :param queue_name: Name of the queue
    :return: Queue URL or None if not found
    """
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
        return response['QueueUrl']
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
            logging.error("Queue '%s' does not exist. Please create it in LocalStack first.", queue_name)
        else:
            logging.error("Error getting queue URL: %s", e)
        return None
    except Exception as e:
        logging.error("Error getting queue URL: %s", e)
        return None


# Mutable container for graceful shutdown (avoids global statement; name is constant)
SHUTDOWN_REQUESTED = [False]


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logging.info("Shutdown signal received. Finishing current operations...")
    SHUTDOWN_REQUESTED[0] = True


def main():
    """
    main Run main setup for local testing with LocalStack SQS
    
    This function polls LocalStack SQS for messages and processes them locally.
    Environment variables:
        - SQS_ENDPOINT_URL: LocalStack endpoint
        - SQS_QUEUE_NAME: Queue name
        - SQS_REGION: AWS region (default: us-east-1)
        - SQS_ACCESS_KEY_ID: AWS access key (default: test)
        - SQS_SECRET_ACCESS_KEY: AWS secret key (default: test)
        - SQS_POLL_WAIT_TIME: Long polling wait time in seconds (default: 20)
        - SQS_VISIBILITY_TIMEOUT: Message visibility timeout in seconds (default: 30)
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting local SQS consumer for <BASE APP> requests...")
    logger.info("This is for local testing only. Use lambda_handler for AWS Lambda deployment.")

    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Get configuration from environment variables
    endpoint_url = os.environ.get('SQS_ENDPOINT_URL', 'http://localhost:4566')
    queue_name = os.environ.get('SQS_QUEUE_NAME')
    region_name = os.environ.get('SQS_REGION', 'us-east-1')
    access_key_id = os.environ.get('SQS_ACCESS_KEY_ID', 'test')
    secret_access_key = os.environ.get('SQS_SECRET_ACCESS_KEY', 'test')
    poll_wait_time = int(os.environ.get('SQS_POLL_WAIT_TIME', '20'))
    visibility_timeout = int(os.environ.get('SQS_VISIBILITY_TIMEOUT', '30'))

    # Create SQS client
    try:
        sqs_client = get_sqs_client(
            endpoint_url=endpoint_url,
            region_name=region_name,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key
        )
    except Exception as e:
        logger.error("Failed to create SQS client: %s", e)
        sys.exit(1)

    # Get queue URL
    queue_url = get_queue_url(sqs_client, queue_name)
    if not queue_url:
        logger.error("Failed to get queue URL for '%s'. Make sure the queue exists in LocalStack.", queue_name)
        sys.exit(1)

    logger.info("Connected to SQS queue")
    logger.info("  Queue Name: %s", queue_name)
    logger.info("  Queue URL: %s", queue_url)
    logger.info("  Endpoint: %s", endpoint_url)
    logger.info("  Region: %s", region_name)
    logger.info("  Poll Wait Time: %d seconds", poll_wait_time)
    logger.info("  Visibility Timeout: %d seconds", visibility_timeout)
    logger.info("Press Ctrl+C to stop...\n")

    processed_count = 0
    failed_count = 0

    # Continuously poll for messages
    while not SHUTDOWN_REQUESTED[0]:
        try:
            # Receive messages from queue
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=poll_wait_time,  # Long polling
                VisibilityTimeout=visibility_timeout
            )

            if 'Messages' in response:
                for message in response['Messages']:
                    receipt_handle = message['ReceiptHandle']
                    message_id = message.get('MessageId', 'unknown')

                    try:
                        logger.info("Processing message: %s", message_id)

                        # Process the message using the same function as Lambda
                        message_body = message['Body']
                        process_sqs_message(message_body, datamap)

                        # Delete message from queue after successful processing
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )

                        processed_count += 1
                        logger.info("Successfully processed and deleted message: %s", message_id)

                    except json.JSONDecodeError as e:
                        logger.error("Error parsing message %s: %s", message_id, e)
                        logger.debug("Raw message body: %s", message.get('Body', ''))
                        failed_count += 1
                        # Delete problematic message to prevent infinite retries
                        try:
                            sqs_client.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=receipt_handle
                            )
                            logger.info("Deleted malformed message: %s", message_id)
                        except Exception as delete_error:
                            logger.error("Failed to delete malformed message: %s", delete_error)

                    except Exception as e:
                        logger.error("Error processing message %s: %s", message_id, e, exc_info=True)
                        failed_count += 1
                        # Note: Message will become visible again after visibility timeout
                        # This allows for retry of transient failures

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Shutting down...")
            break
        except Exception as e:
            logger.error("Error receiving messages: %s", e, exc_info=True)
            if not SHUTDOWN_REQUESTED[0]:
                time.sleep(5)  # Wait before retrying

    logger.info("\n%s", "=" * 80)
    logger.info("Local SQS consumer stopped")
    logger.info("  Processed: %d messages", processed_count)
    logger.info("  Failed: %d messages", failed_count)
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
