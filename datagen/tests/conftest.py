"""Path setup so `import generate` works when pytest runs from anywhere."""
import os
import sys

# Add the datagen directory (parent of tests/) to sys.path so tests can
# `import generate` directly, matching how the container runs it.
_HERE = os.path.dirname(os.path.abspath(__file__))
_DATAGEN = os.path.dirname(_HERE)
if _DATAGEN not in sys.path:
    sys.path.insert(0, _DATAGEN)
