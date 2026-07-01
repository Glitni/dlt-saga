"""SharePoint pipeline for extracting files from Microsoft SharePoint.

Authenticates via Entra ID app-only certificate auth (recommended) or the
deprecated legacy Azure ACS flow, using credentials stored in a secrets
provider (e.g. Azure Key Vault). Supports xlsx, csv, json, and jsonl file types.
"""

from dlt_saga.pipelines.sharepoint.client import SharePointClient
from dlt_saga.pipelines.sharepoint.config import SharePointConfig
from dlt_saga.pipelines.sharepoint.pipeline import SharePointPipeline

__all__ = ["SharePointClient", "SharePointConfig", "SharePointPipeline"]
