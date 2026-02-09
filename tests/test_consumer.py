"""Pytest tests for consumer.py (SQS Lambda consumer)."""

import json
from unittest.mock import MagicMock, patch

import pytest

# Import after potential env setup; module-level logging in consumer is fine for tests
from botocore.exceptions import ClientError

import consumer


class TestProcessSqsMessage:
    """Tests for process_sqs_message."""

    def test_valid_message_with_data(self, sample_message_body, cb_datamap):
        """Valid JSON with 'data' field does not raise."""
        consumer.process_sqs_message(sample_message_body, cb_datamap)

    def test_invalid_json_raises(self, cb_datamap):
        """Invalid JSON raises Exception."""
        with pytest.raises(Exception) as exc_info:
            consumer.process_sqs_message("not valid json", cb_datamap)
        assert "Data Load Error" in str(exc_info.value)

    def test_missing_data_field_raises(self, cb_datamap):
        """Message without 'data' field raises ValueError."""
        body = json.dumps({"metadata": {"source": "test"}})
        with pytest.raises(ValueError, match="missing 'data' field"):
            consumer.process_sqs_message(body, cb_datamap)

    def test_empty_data_allowed(self, cb_datamap):
        """Message with empty 'data' dict is valid."""
        body = json.dumps({"data": {}})
        consumer.process_sqs_message(body, cb_datamap)


class TestLambdaHandler:
    """Tests for lambda_handler."""

    def test_no_records_returns_400(self, mock_lambda_context):
        """Event without 'Records' returns statusCode 400."""
        event = {}
        response = consumer.lambda_handler(event, mock_lambda_context)
        assert response["statusCode"] == 400
        assert response["processed"] == 0
        assert response["failed"] == 0
        assert "No 'Records' found" in response["errors"][0]

    def test_empty_records_success(self, mock_lambda_context):
        """Empty Records list processes none, returns 200."""
        event = {"Records": []}
        response = consumer.lambda_handler(event, mock_lambda_context)
        assert response["statusCode"] == 200
        assert response["processed"] == 0
        assert response["failed"] == 0
        assert response["errors"] == []

    def test_record_without_body_increments_failed(self, mock_lambda_context):
        """Record missing 'body' is counted as failed (all failed -> 500)."""
        event = {"Records": [{"messageId": "m1"}]}  # no 'body'
        response = consumer.lambda_handler(event, mock_lambda_context)
        assert response["statusCode"] == 500  # all failed
        assert response["processed"] == 0
        assert response["failed"] == 1
        assert any("body" in e.lower() for e in response["errors"])

    def test_successful_process_increments_processed(
        self, lambda_event, mock_lambda_context
    ):
        """Valid record is processed and counted."""
        response = consumer.lambda_handler(lambda_event, mock_lambda_context)
        assert response["statusCode"] == 200
        assert response["processed"] == 1
        assert response["failed"] == 0
        assert response["errors"] == []

    def test_invalid_message_in_record_increments_failed(self, mock_lambda_context):
        """Record with invalid message body is failed, error recorded."""
        event = {
            "Records": [
                {
                    "messageId": "m1",
                    "body": "not json",
                }
            ]
        }
        response = consumer.lambda_handler(event, mock_lambda_context)
        assert response["statusCode"] == 500
        assert response["processed"] == 0
        assert response["failed"] == 1
        assert len(response["errors"]) == 1

    def test_missing_data_in_message_increments_failed(self, mock_lambda_context):
        """Record with message missing 'data' is failed."""
        event = {
            "Records": [
                {
                    "messageId": "m1",
                    "body": json.dumps({"metadata": {}}),
                }
            ]
        }
        response = consumer.lambda_handler(event, mock_lambda_context)
        assert response["statusCode"] == 500
        assert response["processed"] == 0
        assert response["failed"] == 1

    def test_partial_success_returns_207(self, sample_message_body, mock_lambda_context):
        """One success and one failure returns 207."""
        event = {
            "Records": [
                {"messageId": "m1", "body": sample_message_body},
                {"messageId": "m2", "body": "invalid"},
            ]
        }
        response = consumer.lambda_handler(event, mock_lambda_context)
        assert response["statusCode"] == 207
        assert response["processed"] == 1
        assert response["failed"] == 1
        assert len(response["errors"]) == 1


class TestGetSqsClient:
    """Tests for get_sqs_client."""

    @patch("consumer.boto3.Session")
    def test_with_endpoint_url_uses_endpoint(self, mock_session):
        """When endpoint_url is set, client is created with endpoint_url."""
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        result = consumer.get_sqs_client(
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
        )
        mock_session.return_value.client.assert_called_once_with(
            "sqs",
            region_name="us-east-1",
            endpoint_url="http://localhost:4566",
        )
        assert result is mock_client

    @patch("consumer.boto3.Session")
    def test_without_endpoint_url_no_endpoint_kwarg(self, mock_session):
        """When endpoint_url is None, client is created without endpoint_url."""
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        result = consumer.get_sqs_client(
            endpoint_url=None,
            region_name="eu-west-1",
        )
        call_kwargs = mock_session.return_value.client.call_args[1]
        assert "endpoint_url" not in call_kwargs
        assert call_kwargs["region_name"] == "eu-west-1"
        assert result is mock_client


class TestGetQueueUrl:
    """Tests for get_queue_url."""

    def test_returns_url_when_found(self):
        """When queue exists, returns QueueUrl."""
        sqs_client = MagicMock()
        sqs_client.get_queue_url.return_value = {"QueueUrl": "http://q/foo"}
        url = consumer.get_queue_url(sqs_client, "my-queue")
        assert url == "http://q/foo"
        sqs_client.get_queue_url.assert_called_once_with(QueueName="my-queue")

    def test_returns_none_for_nonexistent_queue(self):
        """When queue does not exist (NonExistentQueue), returns None."""
        sqs_client = MagicMock()
        sqs_client.get_queue_url.side_effect = ClientError(
            {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue"}},
            "GetQueueUrl",
        )
        url = consumer.get_queue_url(sqs_client, "missing-queue")
        assert url is None

    def test_returns_none_on_other_client_error(self):
        """On other ClientError, returns None."""
        sqs_client = MagicMock()
        sqs_client.get_queue_url.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}},
            "GetQueueUrl",
        )
        url = consumer.get_queue_url(sqs_client, "my-queue")
        assert url is None

    def test_returns_none_on_generic_exception(self):
        """On generic Exception, returns None."""
        sqs_client = MagicMock()
        sqs_client.get_queue_url.side_effect = RuntimeError("network error")
        url = consumer.get_queue_url(sqs_client, "my-queue")
        assert url is None


class TestSignalHandler:
    """Tests for signal_handler."""

    def test_sets_shutdown_requested(self):
        """signal_handler sets SHUTDOWN_REQUESTED[0] to True."""
        consumer.SHUTDOWN_REQUESTED[0] = False
        try:
            consumer.signal_handler(None, None)
            assert consumer.SHUTDOWN_REQUESTED[0] is True
        finally:
            consumer.SHUTDOWN_REQUESTED[0] = False
