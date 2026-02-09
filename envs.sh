# SQS/Lambda Configuration
# Note: SQS queue name is configured in AWS Lambda event source mapping
# No queue connection details needed as Lambda automatically receives SQS events

# Local Testing with LocalStack SQS Configuration
# These are only used when running main() locally, not in Lambda
export SQS_ENDPOINT_URL='http://localhost:24566'
export SQS_QUEUE_NAME='ov_scheduler_queue'
export SQS_REGION='us-east-1'
export SQS_ACCESS_KEY_ID='test'
export SQS_SECRET_ACCESS_KEY='test'
export SQS_POLL_WAIT_TIME='20'  # Long polling wait time in seconds
export SQS_VISIBILITY_TIMEOUT='30'  # Message visibility timeout in seconds

# Logging Configuration
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
