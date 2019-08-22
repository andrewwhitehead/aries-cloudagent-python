"""Base outbound transport."""

from abc import ABC, abstractmethod
import asyncio

from ...messaging.outbound_message import OutboundMessage

from ..error import TransportError


class BaseOutboundTransport(ABC):
    """Base outbound transport class."""

    def __init__(self) -> None:
        """Initialize a `BaseOutboundTransport` instance."""

    async def __aenter__(self):
        """Async context manager enter."""
        await self.start()

    async def __aexit__(self, err_type, err_value, err_t):
        """Async context manager exit."""
        if err_type and err_type != asyncio.CancelledError:
            self.logger.exception("Exception in outbound transport")
        await self.stop()

    @abstractmethod
    async def start(self):
        """Start the transport."""

    @abstractmethod
    async def stop(self):
        """Shut down the transport."""

    @abstractmethod
    async def handle_message(self, message: OutboundMessage):
        """
        Handle message from queue.

        Args:
            message: `OutboundMessage` to send over transport implementation
        """


class OutboundDeliveryError(TransportError):
    """Outbound message delivery error."""


class OutboundTransportRegistrationError(TransportError):
    """Outbound transport registration error."""
