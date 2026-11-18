#!/usr/bin/env python3
"""Apply topology presence semantics to OpenAPI Generator's Python models.

Endpoint/DataConnector are shared by all protocols. Schema defaults may remain
available as model attributes, but must not become explicit configuration when
round-tripping a document. Keep this local policy in the generation/check path;
no upstream generator patch or runtime monkey-patching is required.
"""
from __future__ import annotations

import ast
from pathlib import Path
import sys


MODELS = {"endpoint.py": "Endpoint", "data_connector.py": "DataConnector"}


def apply(path: Path, name: str) -> None:
    text = path.read_text()
    module = ast.parse(text)
    model = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == name)
    methods = {node.name: node for node in model.body if isinstance(node, ast.FunctionDef)}
    # Pydantic validates nested/enumerated fields directly, without populating a
    # synthetic dict containing every optional field and schema default.
    method = methods["from_dict"]
    replacement = [
        '    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:\n',
        '        """Validate supplied fields without materializing omitted defaults."""\n',
        '        if obj is None:\n',
        '            return None\n',
        '        return cls.model_validate(obj)\n',
    ]
    lines = text.splitlines(keepends=True)
    lines[method.lineno - 1:method.end_lineno] = replacement
    text = "".join(lines)
    # Fail closed if the pinned generator's serialization shape changes.
    needle = "            exclude_none=True,\n"
    if text.count(needle) != 1:
        raise ValueError(f"{path}: unexpected model_dump layout")
    if "            exclude_unset=True,\n" not in text:
        text = text.replace(needle, needle + "            exclude_unset=True,\n")
    description = "        * Omitted fields stay omitted; explicit false and zero are retained.\n"
    if description not in text:
        text = text.replace("        * `None` is only added", description + "        * `None` is only added", 1)
    ast.parse(text)
    path.write_text(text)


if __name__ == "__main__":
    directory = Path(sys.argv[1])
    for filename, name in MODELS.items():
        apply(directory / filename, name)
