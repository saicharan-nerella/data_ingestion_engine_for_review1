"""
unstructured_readers — Format-specific ReaderPlugin implementations.

Each file in this module implements a ReaderPlugin that extends BaseUnstructuredReader
and knows how to extract content from exactly one file format into typed ContentBlocks.

ReaderPlugins are registered in core/registry.py under the "unstructured_reader_plugin" layer
and are instantiated and invoked exclusively by the UnifiedParser, never directly by the pipeline.
"""
