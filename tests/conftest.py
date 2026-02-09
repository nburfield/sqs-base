"""Pytest fixtures for consumer tests."""

import json
import sys
from pathlib import Path

import pytest

# Ensure ov-scheduler is on the path so "import consumer" works when running
# pytest from repo root (OV-PROJECT) or from any directory.
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


@pytest.fixture
def sample_message_body():
    """Valid SQS message body (JSON string) with required 'data' field."""
    payload = {
        "data": {"key": "value"},
        "metadata": {
            "timestamp": "2025-10-06T19:48:45.380843",
            "source": "test-source",
            "version": "1.0",
        },
    }
    return json.dumps(payload)


@pytest.fixture
def sample_sqs_record(sample_message_body):
    """Single SQS record as in Lambda event."""
    return {
        "messageId": "msg-123",
        "receiptHandle": "receipt-abc",
        "body": sample_message_body,
        "attributes": {},
    }


@pytest.fixture
def lambda_event(sample_sqs_record):
    """Lambda event with one SQS record."""
    return {"Records": [sample_sqs_record]}


@pytest.fixture
def cb_datamap():
    """Callback datamap passed to process_sqs_message."""
    return {"LOG_LEVEL": "INFO"}


@pytest.fixture
def mock_lambda_context():
    """Minimal Lambda context for handler tests."""
    class Context:
        function_name = "test-function"
        function_version = "1"
        invoked_function_arn = "arn:aws:lambda:us-east-1:123:function:test"
        memory_limit_in_mb = 128
        aws_request_id = "test-request-id"
        log_group_name = "/aws/lambda/test"
        log_stream_name = "stream"

    return Context()
