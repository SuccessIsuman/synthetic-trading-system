"""Compatibility import for the public client."""

from synthetic_trading.deriv_client import DerivApiError, DerivPublicClient, DerivTransportError

__all__ = ["DerivApiError", "DerivPublicClient", "DerivTransportError"]
