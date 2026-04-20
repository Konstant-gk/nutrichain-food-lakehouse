"""
test_upload.py
--------------
Unit tests for src/openfda/upload.py.

We mock the Databricks HTTP API so tests run instantly without
needing real Databricks credentials or a real workspace.
"""

import json
import os
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from src.openfda.upload import upload_run_to_dbfs, get_dbfs_credentials


class TestGetDbfsCredentials:
    """Test that credential loading fails loudly when vars are missing."""

    def test_raises_when_host_missing(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "fake_token")
        with pytest.raises(EnvironmentError, match="DATABRICKS_HOST"):
            get_dbfs_credentials()

    def test_raises_when_token_missing(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "https://fake.databricks.com")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        with pytest.raises(EnvironmentError, match="DATABRICKS_TOKEN"):
            get_dbfs_credentials()

    def test_returns_credentials_when_both_set(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "https://fake.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_fake_token")
        host, token = get_dbfs_credentials()
        assert host == "https://fake.databricks.com"
        assert token == "dapi_fake_token"


class TestUploadRunToDbfs:
    """Test the main upload orchestration function."""

    def _write_fake_json_file(self, folder: str, filename: str, content: dict):
        """Helper: write a small JSON file to a temp folder."""
        filepath = os.path.join(folder, filename)
        with open(filepath, "w") as f:
            json.dump(content, f)
        return filepath

    def test_uploads_all_json_files_in_directory(self, monkeypatch):
        """
        Given 3 JSON files in the local dir, upload_run_to_dbfs should
        make exactly 3 HTTP POST calls to the Databricks API.
        """
        monkeypatch.setenv("DATABRICKS_HOST", "https://fake.community.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_fake")

        with tempfile.TemporaryDirectory() as tmp_dir:
            for i in range(3):
                self._write_fake_json_file(
                    tmp_dir,
                    f"run_test_page_{i+1:04d}.json",
                    {"page": i + 1, "results": []}
                )

            with patch("src.openfda.upload.requests.post") as mock_post:
                fake_response = MagicMock()
                fake_response.raise_for_status.return_value = None
                mock_post.return_value = fake_response

                uploaded = upload_run_to_dbfs(local_dir=tmp_dir, run_id="test_run")

        assert len(uploaded) == 3
        assert mock_post.call_count == 3

    def test_returns_empty_list_when_no_files(self, monkeypatch):
        """
        If the local directory has no JSON files, return empty list
        and make no HTTP calls. No error should be raised.
        """
        monkeypatch.setenv("DATABRICKS_HOST", "https://fake.community.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_fake")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfda.upload.requests.post") as mock_post:
                uploaded = upload_run_to_dbfs(local_dir=tmp_dir, run_id="empty_run")

        assert uploaded == []
        mock_post.assert_not_called()

    def test_dbfs_path_includes_run_id(self, monkeypatch):
        """
        The DBFS upload path must include the run_id as a subfolder.
        This ensures files from different runs stay in separate folders.
        """
        monkeypatch.setenv("DATABRICKS_HOST", "https://fake.community.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_fake")

        with tempfile.TemporaryDirectory() as tmp_dir:
            self._write_fake_json_file(tmp_dir, "run_myrun_page_0001.json", {})

            with patch("src.openfda.upload.requests.post") as mock_post:
                fake_response = MagicMock()
                fake_response.raise_for_status.return_value = None
                mock_post.return_value = fake_response

                upload_run_to_dbfs(local_dir=tmp_dir, run_id="myrun")

            # Extract the JSON body sent to the API.
            call_body = mock_post.call_args[1]["json"]
            assert "myrun" in call_body["path"]