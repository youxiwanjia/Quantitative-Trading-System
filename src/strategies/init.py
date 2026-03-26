"""
Strategies Module Initialization

This module initializes the strategy system.
"""

from .base import BaseStrategy
from .multi_factor import MultiFactorStrategy

__all__ = ['BaseStrategy', 'MultiFactorStrategy']
