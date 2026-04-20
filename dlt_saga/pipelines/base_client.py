from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseClient(ABC):
    """Abstract base class for all data clients"""

    @abstractmethod
    def connect(self, credentials: Optional[Dict[str, Any]] = None):
        """Establish connection to data source"""
        pass

    @abstractmethod
    def test_connection(self):
        """Test connection to source"""
        pass
