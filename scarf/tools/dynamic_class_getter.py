import importlib

from beanie import Document


def get_class(module_name: str, class_name: str) -> object | Document:
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Module '{module_name}' could not be imported: {e}")

    try:
        cls = getattr(module, class_name)
    except AttributeError as e:
        raise AttributeError(f"Class '{class_name}' not found in module '{module_name}': {e}")

    return cls
