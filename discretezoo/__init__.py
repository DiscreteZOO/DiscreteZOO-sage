r"""
DiscreteZOO

DiscreteZOO is an extension for Sage providing access to a database of various
discrete objects with precomputed properties.
"""

__all__ = ["entities", "db"]

# Global settings
DEFAULT_DB = None
WRITE_TO_DB = True
TRACK_CHANGES = True

# Install needed files at startup
from util.install import install
install()

# Globally available names
from entities import *
from db import *

# Initialize global objects
DEFAULT_DB = sqlite.SQLiteDB(track=TRACK_CHANGES)
info = zoograph.info

# Aliases
A = query.A
C = query.Column
V = query.Value
Asc = query.Ascending
Desc = query.Descending
