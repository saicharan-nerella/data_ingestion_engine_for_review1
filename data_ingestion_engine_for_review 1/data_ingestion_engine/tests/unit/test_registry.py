"""
tests/unit/test_registry.py
────────────────────────────
Verifies every registry entry resolves to an importable class.
Run: pytest tests/unit/test_registry.py -v
"""
import importlib
import pytest
from core.registry import REGISTRY


@pytest.mark.parametrize("layer,name,class_path", [
    (layer, name, path)
    for layer, components in REGISTRY.items()
    for name, path in components.items()
])
def test_registry_entry_is_importable(layer, name, class_path):
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    assert cls is not None
