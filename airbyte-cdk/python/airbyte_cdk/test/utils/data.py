# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from pathlib import Path as FilePath


def get_unit_tests_root_folder(execution_folder: FilePath) -> FilePath:
    path = FilePath(execution_folder)
    while path.name != "unit_tests":
        if path.name == path.root or path.name == path.drive:
            raise ValueError(f"Could not find `unit_tests` folder as a parent of {execution_folder}")
        path = path.parent
    return path


def read_resource_file_contents(resource: str, test_location: FilePath) -> str:
    """Read the contents of a test data file from the test resource folder."""
    file_path = str(get_unit_tests_root_folder(test_location) / "resource" / "http" / "response" / f"{resource}")
    with open(file_path) as f:
        response = f.read()
    return response