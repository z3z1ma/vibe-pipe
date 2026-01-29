"""
Tests for webhook handler.
"""

import pytest

from vibe_piper.integration.webhook import (
    SignatureAlgorithm,
    SignatureVerifier,
    WebhookHandler,
    WebhookRequest,
)


@pytest.mark.asyncio
class TestWebhookRequest:
    """Test WebhookRequest wrapper."""

    def test_body_text(self):
        """Test getting body as text."""
        request = WebhookRequest(
            body=b'{"test": "data"}',
            headers={},
            method="POST",
            path="/webhook",
            query_params={},
        )

        assert request.body_text == '{"test": "data"}'

    def test_body_json(self):
        """Test parsing body as JSON."""
        request = WebhookRequest(
            body=b'{"test": "data"}',
            headers={},
            method="POST",
            path="/webhook",
            query_params={},
        )

        data = request.body_json
        assert data == {"test": "data"}

    def test_body_json_invalid(self):
        """Test parsing invalid JSON raises error."""
        request = WebhookRequest(
            body=b"invalid json",
            headers={},
            method="POST",
            path="/webhook",
            query_params={},
        )

        with pytest.raises(ValueError, match="Invalid JSON"):
            _ = request.body_json


@pytest.mark.asyncio
class TestSignatureVerifier:
    """Test webhook signature verification."""

    def test_calculate_signature_sha256(self):
        """Test calculating SHA256 signature."""
        verifier = SignatureVerifier(
            secret="my_secret",
            algorithm=SignatureAlgorithm.HMAC_SHA256,
        )

        data = b'{"test": "data"}'
        signature = verifier._calculate_signature(data)

        assert isinstance(signature, str)
        assert len(signature) == 64  # SHA256 produces 64 hex chars

    def test_verify_valid_signature(self):
        """Test verifying a valid signature."""
        verifier = SignatureVerifier(
            secret="my_secret",
            algorithm=SignatureAlgorithm.HMAC_SHA256,
        )

        data = b'{"test": "data"}'
        expected_signature = verifier._calculate_signature(data)

        request = WebhookRequest(
            body=data,
            headers={"X-Signature": f"sha256={expected_signature}"},
            method="POST",
            path="/webhook",
            query_params={},
        )

        is_valid = verifier.verify(request)
        assert is_valid is True

    def test_verify_invalid_signature(self):
        """Test verifying an invalid signature."""
        verifier = SignatureVerifier(
            secret="my_secret",
            algorithm=SignatureAlgorithm.HMAC_SHA256,
        )

        request = WebhookRequest(
            body=b'{"test": "data"}',
            headers={"X-Signature": "sha256=invalid_signature"},
            method="POST",
            path="/webhook",
            query_params={},
        )

        is_valid = verifier.verify(request)
        assert is_valid is False

    def test_verify_missing_signature_header(self):
        """Test that missing signature header raises error."""
        verifier = SignatureVerifier(secret="my_secret")

        request = WebhookRequest(
            body=b'{"test": "data"}',
            headers={},
            method="POST",
            path="/webhook",
            query_params={},
        )

        with pytest.raises(ValueError, match="Missing signature header"):
            verifier.verify(request)

    def test_empty_secret_raises(self):
        """Test that empty secret raises ValueError."""
        with pytest.raises(ValueError, match="Secret cannot be empty"):
            SignatureVerifier(secret="")


@pytest.mark.asyncio
class TestWebhookHandler:
    """Test webhook handler."""

    async def test_register_handler_with_decorator(self):
        """Test registering handler with decorator."""
        handler = WebhookHandler()

        @handler.on("user.created")
        async def handle_user_created(request: WebhookRequest):
            data = request.body_json
            return data

        assert "user.created" in handler.get_supported_events()

    async def test_register_default_handler(self):
        """Test registering default handler."""
        handler = WebhookHandler()

        @handler.default
        async def handle_default(request: WebhookRequest):
            return "default"

        assert handler._default_handler is not None

    async def test_handle_event(self):
        """Test handling an event."""
        handler = WebhookHandler(require_signature=False)

        received_data = {}

        @handler.on("user.created")
        async def handle_user_created(request: WebhookRequest):
            data = request.body_json
            received_data["user_id"] = data["user_id"]
            return "success"

        request = WebhookRequest(
            body=b'{"user_id": 123}',
            headers={"X-Event-Type": "user.created"},
            method="POST",
            path="/webhook",
            query_params={},
        )

        result = await handler.handle(request)

        assert result == "success"
        assert received_data["user_id"] == 123

    async def test_handle_with_signature_verification(self):
        """Test handling event with signature verification."""
        verifier = SignatureVerifier(
            secret="test_secret",
            algorithm=SignatureAlgorithm.HMAC_SHA256,
        )
        handler = WebhookHandler(verifier=verifier, require_signature=True)

        @handler.on("test.event")
        async def handle_test(request: WebhookRequest):
            return "success"

        data = b'{"test": "data"}'
        signature = verifier._calculate_signature(data)

        request = WebhookRequest(
            body=data,
            headers={
                "X-Signature": f"sha256={signature}",
                "X-Event-Type": "test.event",
            },
            method="POST",
            path="/webhook",
            query_params={},
        )

        result = await handler.handle(request)
        assert result == "success"

    async def test_handle_invalid_signature_raises(self):
        """Test that invalid signature raises error."""
        verifier = SignatureVerifier(secret="test_secret")
        handler = WebhookHandler(verifier=verifier, require_signature=True)

        @handler.on("test.event")
        async def handle_test(request: WebhookRequest):
            return "success"

        request = WebhookRequest(
            body=b'{"test": "data"}',
            headers={
                "X-Signature": "invalid_signature",
                "X-Event-Type": "test.event",
            },
            method="POST",
            path="/webhook",
            query_params={},
        )

        with pytest.raises(ValueError, match="Invalid webhook signature"):
            await handler.handle(request)

    async def test_handle_unsupported_event_with_default(self):
        """Test handling unsupported event with default handler."""
        handler = WebhookHandler(require_signature=False)

        @handler.default
        async def handle_default(request: WebhookRequest):
            return "default_handled"

        request = WebhookRequest(
            body=b"{}",
            headers={"X-Event-Type": "unsupported.event"},
            method="POST",
            path="/webhook",
            query_params={},
        )

        result = await handler.handle(request)
        assert result == "default_handled"

    async def test_handle_unsupported_event_without_default(self):
        """Test handling unsupported event without default handler."""
        handler = WebhookHandler(require_signature=False)

        request = WebhookRequest(
            body=b"{}",
            headers={"X-Event-Type": "unsupported.event"},
            method="POST",
            path="/webhook",
            query_params={},
        )

        result = await handler.handle(request)
        assert result is None

    async def test_get_supported_events(self):
        """Test getting list of supported events."""
        handler = WebhookHandler()

        @handler.on("event1")
        async def handle1(request: WebhookRequest):
            pass

        @handler.on("event2")
        async def handle2(request: WebhookRequest):
            pass

        events = handler.get_supported_events()

        assert "event1" in events
        assert "event2" in events
        assert len(events) == 2
