"""Reports module for Lakebench.

Generates HTML benchmark reports from collected metrics.
"""

from .generator import ReportGenerator

__all__ = [
    "ReportGenerator",
]
