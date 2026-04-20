"""SharePoint pipeline for extracting files from Microsoft SharePoint.

Authenticates via the SharePoint app-only OAuth 2.0 flow using credentials
stored in a secrets provider (e.g. Azure Key Vault). Supports xlsx, csv,
json, and jsonl file types.
"""

from dlt_saga.pipelines.sharepoint.client import SharePointClient
from dlt_saga.pipelines.sharepoint.config import SharePointConfig
from dlt_saga.pipelines.sharepoint.pipeline import SharePointPipeline

__all__ = ["SharePointClient", "SharePointConfig", "SharePointPipeline"]
