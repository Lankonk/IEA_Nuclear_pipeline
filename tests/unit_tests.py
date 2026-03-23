import pytest
from unittest.mock import patch, MagicMock
from extract_nuclear_data import validate_data_with_sampling
from delta_pyspark import upsert_to_delta

class TestExtraction:
    """Tests for the API extraction and validation layer."""

    def test_validation_sampling_logic(self):
        # Data with required fields
        good_data = [{"period": "2026-03-01", "outage": 100, "facility": "Ginna"}]
        bad_data = [{"outage": 100}] # Missing period/facility
        
        keys = ["period", "outage", "facility"]
        
        assert validate_data_with_sampling(good_data, keys, 1.0) is True
        assert validate_data_with_sampling(bad_data, keys, 1.0) is False

    @patch('requests.Session.get')
    def test_eia_api_pagination_stop(self, mock_get):
        """Ensures the extractor stops when no data is returned."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"response": {"data": [], "total": 0}}
        mock_get.return_value = mock_response
        
        from extract_nuclear_data import get_nuclear_outages
        res = get_nuclear_outages("us-nuclear-outages")
        assert res == []

class TestTransformation:
    """Tests for Spark-based Silver layer processing."""

    def test_outage_casting_to_double(self, spark):
        # Raw outage is often a string from APIs
        raw = [{"period": "2026-03-01", "outage": "450.5"}]
        df = upsert_to_delta(raw, "test_outage")
        
        # Verify double casting logic
        assert df.schema["outage"].dataType.simpleString() == "double"
        assert df.collect()[0]["outage"] == 450.5

class TestAPI:
    """Tests for the FastAPI serving layer."""

    def test_get_data_success(self, client):
        # Setup mock database return value
        mock_cursor = client.mock_db.cursor.return_value
        mock_cursor.fetchall.return_value = [{"period": "2026-03-01", "outage": 100}]
        
        response = client.get("/data?dataset=us-nuclear-outages")
        assert response.status_code == 200
        assert response.json()[0]["outage"] == 100

    def test_invalid_dataset_fails(self, client):
        response = client.get("/data?dataset=invalid-set")
        assert response.status_code == 400 # Handled in api.py