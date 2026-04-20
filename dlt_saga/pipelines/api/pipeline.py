"""Generic API pipeline implementation.

This pipeline uses BaseApiPipeline to handle generic REST API sources.
For APIs that need custom logic, create a subdirectory under api/
with its own pipeline.py.
"""

from dlt_saga.pipelines.api.base import BaseApiPipeline


class ApiPipeline(BaseApiPipeline):
    """Generic API pipeline for simple REST API sources.

    This pipeline handles:
    - HTTP requests with various auth methods
    - JSON response extraction via response_path
    - Standard dlt loading to destination

    For APIs requiring custom logic (e.g., pagination, custom transformations),
    create a specific implementation:
    - pipelines/api/my_api/pipeline.py → MyApiPipeline
    """

    pass  # Uses all functionality from BaseApiPipeline
