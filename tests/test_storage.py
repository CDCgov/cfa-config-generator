import pytest

from src.cfa_config_generator.utils.azure.storage import prep_blob_path


@pytest.mark.parametrize(
    "blob_path, expected_container, expected_blob, should_fail",
    [
        ("az://container/blob", "container", "blob", False),
        ("az://container-2/subdir/blob", "container-2", "subdir/blob", False),
        ("az://container/subdir/", "container", "subdir/", False),
        ("az://container/subdir", "container", "subdir", False),
        ("az://container/", "", "", True),
        ("az://container", "", "", True),
        ("invalid_path", "", "", True),
        ("", "", "", True),
    ],
)
def test_prep_blob_path(
    blob_path: str,
    expected_container: str,
    expected_blob: str,
    should_fail: bool,
):
    """
    Make sure we parse the blob path correctly.
    """
    if should_fail:
        with pytest.raises(ValueError):
            prep_blob_path(blob_path)
        return

    container, blob = prep_blob_path(blob_path)
    assert expected_container == container
    assert expected_blob == blob
