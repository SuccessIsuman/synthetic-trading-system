"""Reliable, read-only market-data collection for Deriv synthetic indices."""

from .deriv_client import DerivApiError, DerivPublicClient, DerivTransportError

__all__ = ["DerivApiError", "DerivPublicClient", "DerivTransportError"]
