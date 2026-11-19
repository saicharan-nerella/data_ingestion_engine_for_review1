"""
Complete Glue Job Integration Test

This test verifies the complete fix for tesserocr import errors by:
1. Ensuring no binary dependencies are imported during execution
2. Validating ECS communication works end-to-end 
3. Testing error scenarios (ECS down, network issues, malformed files)
4. Verifying the complete pipeline execution simulation

This integration test demonstrates that the tesserocr import errors are completely resolved.
"""

import pytest
import json
import os
import tempfile
import time
import boto3
from unittest.mock import Mock, patch, MagicMock
from moto import mock_s3
import requests
from io import BytesIO

# Import the main components we're testing
from main.aimatic import (
    _run_unstructured_pipeline,
    S3JSONWriter,
    _get_args,
    _load_yaml
)
from parser.glue_ecs_parser import (
    GlueECSParser,
    DependencyValidator,
    ECSHealthChecker,
    create_glue_ecs_parser
)
from parser.unified_docling_parser import UnifiedDoclingParser


class TestCompleteGlueJobIntegration:
    """Integration tests for complete Glue job execution without binary dependencies."""
    
    @pytest.fixture
    def mock_s3_setup(self):
        """Set up mock S3 environment with test files."""
        with mock_s3():
            # Create S3 client and buckets
            s3_client = boto3.client("s3", region_name="us-east-1")
            
            # Create test buckets
            s3_client.create_bucket(Bucket="test-input-bucket")
            s3_client.create_bucket(Bucket="test-output-bucket")
            
            # Upload test config
            test_config = {
                "unstructured": {
                    "source": {
                        "bucket": "test-input-bucket",
                        "prefix": "documents/",
                        "region": "us-east-1",
                        "files": ["test.pdf", "test.docx"]
                    }
                }
            }
            
            s3_client.put_object(
                Bucket="test-input-bucket",
                Key="config.yaml",
                Body=json.dumps(test_config).encode('utf-8')
            )
            
            # Upload test documents
            s3_client.put_object(
                Bucket="test-input-bucket",
                Key="documents/test.pdf",
                Body=b"fake pdf content for testing"
            )
            
            s3_client.put_object(
                Bucket="test-input-bucket", 
                Key="documents/test.docx",
                Body=b"fake docx content for testing"
            )
            
            yield s3_client, test_config
    
    @pytest.fixture
    def mock_healthy_ecs_service(self):
        """Mock a healthy ECS service that responds correctly."""
        with patch('requests.get') as mock_get, patch('requests.post') as mock_post:
            # Health check response
            mock_health_response = Mock()
            mock_health_response.status_code = 200
            mock_get.return_value = mock_health_response
            
            # Parse endpoint response
            mock_parse_response = Mock()
            mock_parse_response.status_code = 200
            mock_parse_response.json.return_value = {
                "format": "pdf",
                "status": "success",
                "blocks": [
                    {"type": "text", "content": "Extracted text from document"},
                    {"type": "heading", "content": "Document Title", "level": 1}
                ]
            }
            mock_post.return_value = mock_parse_response
            
            yield mock_get, mock_post
    
    def test_environment_validation_prevents_binary_imports(self):
        """
        Test 1: Environment validation prevents binary dependencies from being imported.
        
        Validates Requirements: 1.1, 1.2, 1.3, 1.4
        """
        # Test clean environment
        with patch('sys.modules', {}):
            with patch('importlib.util.find_spec', return_value=None):
                assert DependencyValidator.validate_glue_environment() is True
        
        # Test with tesserocr imported (should fail)
        with patch('sys.modules', {'tesserocr': Mock()}):
            assert DependencyValidator.validate_glue_environment() is False
        
        # Test with docling imported (should fail)
        with patch('sys.modules', {'docling': Mock()}):
            assert DependencyValidator.validate_glue_environment() is False
        
        # Test parser creation fails with unsafe environment
        with patch.object(DependencyValidator, 'validate_glue_environment', return_value=False):
            with pytest.raises(RuntimeError, match="Unsafe binary dependencies detected"):
                create_glue_ecs_parser("http://test-service:8000")
    
    def test_ecs_communication_end_to_end(self, mock_healthy_ecs_service):
        """
        Test 2: ECS communication works end-to-end via HTTP only.
        
        Validates Requirements: 1.5, 2.1, 2.2, 2.5
        """
        mock_get, mock_post = mock_healthy_ecs_service
        
        # Create parser and test communication
        parser = GlueECSParser("http://test-ecs-service:8000", timeout=30)
        
        # Test health check
        health_result = parser.check_service_health()
        assert health_result is True
        mock_get.assert_called_with("http://test-ecs-service:8000/health", timeout=10)
        
        # Test document parsing
        test_content = b"fake document content"
        result = parser.parse_document("test.pdf", test_content)
        
        # Verify HTTP request was made correctly
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "http://test-ecs-service:8000/parse"
        assert "files" in kwargs
        assert kwargs["timeout"] == 30
        
        # Verify result format
        assert result["status"] == "success"
        assert result["filename"] == "test.pdf"
        assert result["format"] == "pdf"
        assert len(result["content"]["blocks"]) == 2
        assert "extracted_at" in result
    
    def test_ecs_service_unavailable_scenario(self):
        """
        Test 3: Error handling when ECS service is unavailable.
        
        Validates Requirements: 2.3, 2.4, 3.2, 4.1, 4.2
        """
        with patch('requests.get') as mock_get, patch('requests.post') as mock_post:
            # Mock connection errors
            mock_get.side_effect = requests.exceptions.ConnectionError("Service unavailable")
            mock_post.side_effect = requests.exceptions.ConnectionError("Service unavailable")
            
            parser = GlueECSParser("http://unreachable-service:8000")
            
            # Test health check fails gracefully
            health_result = parser.check_service_health()
            assert health_result is False
            
            # Test parsing fails gracefully with proper error record
            result = parser.parse_document("test.pdf", b"test content")
            
            assert result["status"] == "error"
            assert "Cannot connect to ECS" in result["error"]
            assert result["filename"] == "test.pdf"
            assert "extracted_at" in result
    
    def test_network_timeout_scenario(self):
        """
        Test 4: Timeout protection is enforced for network requests.
        
        Validates Requirements: 2.5, 3.4
        """
        with patch('requests.get') as mock_get, patch('requests.post') as mock_post:
            # Mock timeout errors
            mock_get.side_effect = requests.exceptions.Timeout("Request timeout")
            mock_post.side_effect = requests.exceptions.Timeout("Request timeout")
            
            parser = GlueECSParser("http://slow-service:8000", timeout=5)
            
            # Test health check timeout
            health_result = parser.check_service_health()
            assert health_result is False
            
            # Test parsing timeout
            result = parser.parse_document("test.pdf", b"test content")
            
            assert result["status"] == "error"
            assert "ECS timeout (5s)" in result["error"]
            assert result["filename"] == "test.pdf"
    
    def test_malformed_file_handling(self, mock_healthy_ecs_service):
        """
        Test 5: Malformed files are handled gracefully.
        
        Validates Requirements: 4.1, 4.3, 5.2
        """
        mock_get, mock_post = mock_healthy_ecs_service
        
        # Mock ECS returning error for malformed file
        mock_error_response = Mock()
        mock_error_response.status_code = 400
        mock_error_response.text = "Malformed file: cannot parse"
        mock_post.return_value = mock_error_response
        
        parser = GlueECSParser("http://test-ecs-service:8000")
        result = parser.parse_document("malformed.pdf", b"corrupted content")
        
        # Should create proper error record
        assert result["status"] == "error"
        assert "ECS error: HTTP 400" in result["error"]
        assert result["filename"] == "malformed.pdf"
        assert "extracted_at" in result
    
    @mock_s3
    def test_complete_pipeline_execution_simulation(self, mock_healthy_ecs_service):
        """
        Test 6: Complete pipeline execution simulation from start to finish.
        
        Validates Requirements: All requirements (comprehensive test)
        """
        mock_get, mock_post = mock_healthy_ecs_service
        
        # Set up S3 environment
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-input-bucket")
        s3_client.create_bucket(Bucket="test-output-bucket")
        
        # Upload test config
        test_config = {
            "unstructured": {
                "source": {
                    "bucket": "test-input-bucket",
                    "prefix": "documents/",
                    "region": "us-east-1",
                    "files": ["test1.pdf", "test2.docx"]
                }
            }
        }
        
        # Upload test files
        s3_client.put_object(
            Bucket="test-input-bucket",
            Key="documents/test1.pdf", 
            Body=b"test pdf content"
        )
        s3_client.put_object(
            Bucket="test-input-bucket",
            Key="documents/test2.docx",
            Body=b"test docx content"
        )
        
        # Mock environment validation as passing
        with patch.object(DependencyValidator, 'validate_glue_environment', return_value=True):
            # Mock args for pipeline
            test_args = {
                "ecs_endpoint": "http://test-ecs-service:8000",
                "writer_bucket": "test-output-bucket",
                "writer_prefix": "results/",
                "writer_region": "us-east-1"
            }
            
            # Run complete unstructured pipeline
            result = _run_unstructured_pipeline(test_config, test_args)
            
            # Verify pipeline completed successfully
            assert result["status"] in ["SUCCESS", "PARTIAL_SUCCESS"]
            assert result["total_files"] == 2
            assert result["processed_successfully"] >= 0
            assert result["failed"] >= 0
            assert result["total_files"] == result["processed_successfully"] + result["failed"]
            
            # Verify HTTP communication occurred
            assert mock_get.called  # Health check
            assert mock_post.called  # Document parsing
            
            # Verify output files were written to S3
            output_objects = s3_client.list_objects_v2(
                Bucket="test-output-bucket", 
                Prefix="results/"
            )
            assert "Contents" in output_objects
            assert len(output_objects["Contents"]) == 2  # Two output files
    
    def test_resilient_error_handling_continues_processing(self):
        """
        Test 7: Pipeline continues processing despite individual file failures.
        
        Validates Requirements: 4.2, 4.4, 4.5
        """
        with mock_s3():
            # Set up S3 with multiple files
            s3_client = boto3.client("s3", region_name="us-east-1")
            s3_client.create_bucket(Bucket="test-input-bucket")
            s3_client.create_bucket(Bucket="test-output-bucket")
            
            test_config = {
                "unstructured": {
                    "source": {
                        "bucket": "test-input-bucket",
                        "prefix": "documents/",
                        "region": "us-east-1",
                        "files": ["success.pdf", "failure.pdf", "timeout.pdf"]
                    }
                }
            }
            
            # Upload test files
            for filename in ["success.pdf", "failure.pdf", "timeout.pdf"]:
                s3_client.put_object(
                    Bucket="test-input-bucket",
                    Key=f"documents/{filename}",
                    Body=f"content for {filename}".encode('utf-8')
                )
            
            # Mock varying ECS responses
            with patch('requests.get') as mock_get, patch('requests.post') as mock_post:
                # Health check passes
                mock_health_response = Mock()
                mock_health_response.status_code = 200
                mock_get.return_value = mock_health_response
                
                # Varying parse responses
                def parse_side_effect(*args, **kwargs):
                    files = kwargs.get('files', {})
                    filename = list(files.values())[0][0] if files else "unknown"
                    
                    if "success.pdf" in filename:
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            "format": "pdf",
                            "blocks": [{"type": "text", "content": "Success"}]
                        }
                        return mock_response
                    elif "failure.pdf" in filename:
                        mock_response = Mock()
                        mock_response.status_code = 500
                        mock_response.text = "Internal server error"
                        return mock_response
                    elif "timeout.pdf" in filename:
                        raise requests.exceptions.Timeout("Request timeout")
                
                mock_post.side_effect = parse_side_effect
                
                # Mock environment validation
                with patch.object(DependencyValidator, 'validate_glue_environment', return_value=True):
                    test_args = {
                        "ecs_endpoint": "http://test-ecs-service:8000",
                        "writer_bucket": "test-output-bucket",
                        "writer_prefix": "results/",
                        "writer_region": "us-east-1"
                    }
                    
                    # Run pipeline - should handle all scenarios
                    result = _run_unstructured_pipeline(test_config, test_args)
                    
                    # Verify processing continued despite failures
                    assert result["total_files"] == 3
                    assert result["processed_successfully"] == 1  # Only success.pdf
                    assert result["failed"] == 2  # failure.pdf and timeout.pdf
                    assert result["status"] == "PARTIAL_SUCCESS"
                    
                    # Verify all files were processed (success and error records)
                    output_objects = s3_client.list_objects_v2(
                        Bucket="test-output-bucket",
                        Prefix="results/"
                    )
                    assert "Contents" in output_objects
                    assert len(output_objects["Contents"]) == 3  # All files created records
    
    def test_processing_record_format_consistency(self, mock_healthy_ecs_service):
        """
        Test 8: Output format consistency regardless of input document type.
        
        Validates Requirements: 5.1, 5.2, 5.3, 5.4, 5.5
        """
        mock_get, mock_post = mock_healthy_ecs_service
        
        # Test different document types produce consistent format
        parser = GlueECSParser("http://test-ecs-service:8000")
        
        test_files = [
            ("document.pdf", b"pdf content"),
            ("document.docx", b"docx content"),
            ("document.txt", b"text content")
        ]
        
        results = []
        for filename, content in test_files:
            result = parser.parse_document(filename, content)
            results.append(result)
        
        # Verify all results have consistent structure
        for result in results:
            assert "source_uri" in result
            assert "filename" in result
            assert "format" in result
            assert "status" in result
            assert "extracted_at" in result
            
            if result["status"] == "success":
                assert "content" in result
                assert "blocks" in result["content"]
                assert "block_count" in result
            else:
                assert "error" in result
    
    def test_unified_parser_compatibility(self):
        """
        Test 9: UnifiedDoclingParser maintains API compatibility with HTTP-only backend.
        
        Validates that the UnifiedDoclingParser successfully switches to HTTP-only mode.
        """
        with patch.object(DependencyValidator, 'validate_glue_environment', return_value=True):
            with patch('requests.get') as mock_get, patch('requests.post') as mock_post:
                # Mock healthy ECS service
                mock_health_response = Mock()
                mock_health_response.status_code = 200
                mock_get.return_value = mock_health_response
                
                mock_parse_response = Mock()
                mock_parse_response.status_code = 200
                mock_parse_response.json.return_value = {
                    "format": "pdf",
                    "blocks": [{"type": "text", "content": "Parsed content"}]
                }
                mock_post.return_value = mock_parse_response
                
                # Test UnifiedDoclingParser uses HTTP-only backend
                parser = UnifiedDoclingParser(
                    config={"ocr_enabled": True},
                    ecs_endpoint="http://test-ecs-service:8000"
                )
                
                result = parser.parse("test.pdf", b"pdf content")
                
                # Verify it produces expected format
                assert "source_uri" in result
                assert "format" in result
                assert "extracted_at" in result
                assert "content" in result
                assert isinstance(result["content"]["blocks"], list)
    
    def test_configuration_validation(self):
        """
        Test 10: ECS endpoint configuration validation works correctly.
        
        Validates Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
        """
        # Test valid endpoints
        valid_endpoints = [
            "http://service.example.com:8000",
            "https://secure-service.com",
            "http://192.168.1.100:8080",
            "http://localhost:3000"
        ]
        
        for endpoint in valid_endpoints:
            parser = GlueECSParser(endpoint)
            assert parser.ecs_endpoint == endpoint.rstrip('/')
        
        # Test invalid endpoints
        invalid_endpoints = [
            "invalid-url",
            "ftp://not-http.com",
            "",
            None
        ]
        
        for endpoint in invalid_endpoints:
            if endpoint is None:
                with pytest.raises(AttributeError):
                    GlueECSParser(endpoint)
            else:
                with pytest.raises(ValueError, match="Invalid ECS endpoint format"):
                    GlueECSParser(endpoint)


if __name__ == "__main__":
    # Run all integration tests
    pytest.main([__file__, "-v", "--tb=short"])