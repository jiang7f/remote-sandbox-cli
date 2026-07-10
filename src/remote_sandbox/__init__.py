"""Remote Sandbox CLI package."""

from importlib.metadata import PackageNotFoundError, version

from .namespace import DEV_NAMESPACE

__all__ = ["__version__"]

try:
    __version__ = version(DEV_NAMESPACE.distribution)
except PackageNotFoundError:  # pragma: no cover - editable source tree without install metadata
    __version__ = "0.0.0"
