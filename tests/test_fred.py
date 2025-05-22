import pytest
from fred import Escalation # Assuming fred.py is in the PYTHONPATH or project root
import os
import pandas as pd
from unittest.mock import patch, Mock

@pytest.fixture
def escalation_instance():
    """Provides an instance of the Escalation class with a mock API key."""
    # Set a dummy API key for testing if os.environ.get is used directly
    # If the class takes api_key as an __init__ arg after refactor, adjust this
    with patch.dict(os.environ, {"FRED_API_KEY": "test_api_key"}):
        instance = Escalation()
    return instance

def test_escalation_initialization(escalation_instance):
    """Test basic initialization of Escalation class."""
    assert escalation_instance is not None
    assert escalation_instance._api_key == "test_api_key" # or however it's stored
    assert isinstance(escalation_instance._series, pd.DataFrame) # Check _series initialization

@patch('fred.Escalation._request_json') # Patch the actual method making the HTTP request
def test_get_series_from_search_mocked(mock_request_json, escalation_instance):
    """Test get_series_from_search with a mocked API response."""
    # Mock responses for the sequence of calls made by get_series_from_search -> search_series -> get_series -> get_series_info -> get_series_sources -> _request_df -> _request_json
    mock_request_json.side_effect = [
        # 1. Call from search_series (for self.search_series(term, limit=limit, **kwargs))
        { 
            'seriess': [{'id': 'GDP', 'title': 'Gross Domestic Product', 'units': 'Billions of Dollars', 'frequency': 'Quarterly', 'seasonal_adjustment': 'Seasonally Adjusted'}]
        },
        # Calls from get_series(ids, **kwargs) for 'GDP':
        # 2. Call from get_series_info (main_series_info_df = self._request_df)
        { 
            'seriess': [{'id': 'GDP', 'title': 'Gross Domestic Product', 'units': 'Billions of Dollars'}]
        },
        # 3. Call from get_series_sources -> get_series_release
        {
          'releases': [{'id': 'release123', 'name': 'GDP Release', 'link': 'http://example.com/gdp_release'}]
        },
        # 4. Call from get_series_sources -> _request_df for sources
        {
          'sources': [{'id': 'source1', 'name': 'BEA', 'link': 'http://bea.gov'}]
        },
        # 5. Call from get_series -> _request_df for observations
        { 
            'observations': [{'date': '2023-01-01', 'value': '27000.000'}, {'date': '2023-04-01', 'value': '27500.000'}]
        }
    ]

    result_df = escalation_instance.get_series_from_search("GDP", limit=1)

    assert not result_df.empty
    # The 'id' column in the final DataFrame from get_series is the one from the info part, not the search part directly.
    # It should be 'GDP' as per the mocked info.
    assert 'GDP' in result_df['id_info'].values # based on suffixes=("_info", "_obs") and how get_series structures it
    assert 'value' in result_df.columns
    
    # Verify that _request_json was called multiple times as expected
    # search -> 1 call
    # get_series -> get_series_info -> _request_df (1 call)
    # get_series_info -> get_series_sources -> get_series_release -> _request_df (1 call)
    # get_series_info -> get_series_sources -> _request_df for sources (1 call)
    # get_series -> _request_df for observations (1 call)
    # Total = 5 calls
    assert mock_request_json.call_count == 5


# Add a placeholder test for escalation.py if simple logic can be isolated
# For now, focus on fred.py as it has more testable standalone logic.
# To test Panel apps, more complex setups (e.g., using panel.io.server.Server directly
# or specific Panel testing tools if they exist) would be needed.
