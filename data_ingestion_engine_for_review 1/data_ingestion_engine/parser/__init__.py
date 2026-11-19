"""
parser — HTTP-only parser for ECS Fargate service.

The ECSHttpParser is the single parser needed for all unstructured formats.
It communicates with the ECS Fargate service via HTTP to avoid binary
dependency conflicts in AWS Glue.

Usage:
    from parser.ecs_http_parser import ECSHttpParser
    
    parser = ECSHttpParser({"ecs_endpoint": "http://service:8000"})
    result = parser.extract(object_key, file_bytes)
"""

from .ecs_http_parser import ECSHttpParser

__all__ = ["ECSHttpParser"]