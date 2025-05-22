import pytest
from fred import Escalation
import os
import pandas as pd
import numpy as np # Added numpy
from unittest.mock import patch, Mock, call # Added call for checking mock calls
import pytest

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

# --- Fixture for Sample Historical Data ---
@pytest.fixture
def sample_historical_data() -> pd.DataFrame:
    """Provides a sample DataFrame with historical data for testing make_index."""
    dates = pd.to_datetime([f'20{i:02d}-12-31' for i in range(10, 24)]) # Annual data 2010 to 2023
    values = np.array([100 + i*2 + np.sin(i/2)*5 for i in range(len(dates))]) # Trend with some seasonality
    
    df = pd.DataFrame({'date': dates, 'value': values})
    df['rate'] = df['value'].pct_change() # Using pct_change for rate calculation
    # df['rate'] = df['value'] / df['value'].shift(1) - 1 # Alternative rate calculation
    
    df['FY'] = df['date'].dt.year
    df['id'] = 'TESTS1' # Series ID for this data
    df['title'] = 'Test Series Alpha'
    df['units'] = 'Index'
    df = df.dropna().reset_index(drop=True)
    return df

# --- Tests for make_index with New Forecasting ---

def test_make_index_median_forecast(escalation_instance, sample_historical_data):
    """Test make_index with median forecasting option."""
    # sample_historical_data ends in FY 2023. We want to forecast beyond that.
    # fy_range output will be up to 2028. Forecasting for 2024-2028.
    future_fy_start = sample_historical_data['FY'].max() + 1
    future_fy_end = future_fy_start + 4 # Forecast 5 years
    
    index_df = escalation_instance.make_index(
        df=sample_historical_data.copy(), # Pass the DataFrame directly
        base_year=2022,
        outlays=(0.5, 0.5), # Simplified outlays
        fy_range=(sample_historical_data['FY'].min(), future_fy_end), # Ensure fy_range covers forecast period
        forecast_options={'method': 'median'}
    )
    assert not index_df.empty
    assert 'forecast_method_used' in index_df.columns
    assert index_df['forecast_method_used'].iloc[0] == 'median'
    
    # Check rates for future years
    future_rates = index_df[index_df['FY'] >= future_fy_start]['rate']
    expected_median_rate = sample_historical_data['rate'].median()
    assert np.allclose(future_rates, expected_median_rate)
    assert len(future_rates) == (future_fy_end - future_fy_start + 1)

def test_make_index_mean_forecast(escalation_instance, sample_historical_data):
    """Test make_index with mean forecasting option."""
    future_fy_start = sample_historical_data['FY'].max() + 1
    future_fy_end = future_fy_start + 4
    
    index_df = escalation_instance.make_index(
        df=sample_historical_data.copy(),
        base_year=2022,
        outlays=(0.5, 0.5),
        fy_range=(sample_historical_data['FY'].min(), future_fy_end),
        forecast_options={'method': 'mean'}
    )
    assert not index_df.empty
    assert index_df['forecast_method_used'].iloc[0] == 'mean'
    future_rates = index_df[index_df['FY'] >= future_fy_start]['rate']
    expected_mean_rate = sample_historical_data['rate'].mean()
    assert np.allclose(future_rates, expected_mean_rate)

# No mock needed here if df is provided and series_id is not used to fetch primary data
def test_make_index_arima_forecast(escalation_instance, sample_historical_data):
    """Test make_index with ARIMA forecasting option."""
    future_fy_start = sample_historical_data['FY'].max() + 1
    future_fy_end = future_fy_start + 4

    index_df = escalation_instance.make_index(
        df=sample_historical_data.copy(), 
        base_year=2022,
        outlays=(0.5, 0.5),
        fy_range=(sample_historical_data['FY'].min(), future_fy_end),
        forecast_options={'method': 'arima', 'order': (1, 1, 0)} # Example order
    )
    assert not index_df.empty
    assert 'forecast_method_used' in index_df.columns
    # The method used should be 'arima' as requested, even if specific values are hard to pin down without running ARIMA
    assert index_df['forecast_method_used'].iloc[0] == 'arima' 
    future_rates = index_df[index_df['FY'] >= future_fy_start]['rate']
    assert not future_rates.empty
    assert not future_rates.isna().any() # Ensure some forecast was made

# No mock needed here if df is provided and series_id is not used to fetch primary data
def test_make_index_exponential_smoothing_forecast(escalation_instance, sample_historical_data):
    """Test make_index with Exponential Smoothing forecasting."""
    future_fy_start = sample_historical_data['FY'].max() + 1
    future_fy_end = future_fy_start + 4

    index_df = escalation_instance.make_index(
        df=sample_historical_data.copy(),
        base_year=2022,
        outlays=(0.5, 0.5),
        fy_range=(sample_historical_data['FY'].min(), future_fy_end),
        forecast_options={'method': 'exponential_smoothing', 'trend': 'add', 'seasonal': None}
    )
    assert not index_df.empty
    assert index_df['forecast_method_used'].iloc[0] == 'exponential_smoothing'
    future_rates = index_df[index_df['FY'] >= future_fy_start]['rate']
    assert not future_rates.empty
    assert not future_rates.isna().any()

@patch('fred.Escalation.get_series') # Mock get_series directly for chained forecast
def test_make_index_chained_forecast(mock_get_series, escalation_instance, sample_historical_data):
    """Test make_index with chained forecasting option."""
    future_fy_start = sample_historical_data['FY'].max() + 1
    future_fy_end = future_fy_start + 4

    # Prepare mock data for the chained series ('GDPPI')
    # Ensure it has 'date', 'value', 'rate', 'FY', 'id', 'title', 'units'
    chained_dates = pd.to_datetime([f'20{i:02d}-12-31' for i in range(10, 24)]) # Same date range for simplicity
    chained_values = np.array([50 + i*1.5 for i in range(len(chained_dates))]) # Different values
    mock_chained_df = pd.DataFrame({'date': chained_dates, 'value': chained_values})
    mock_chained_df['rate'] = mock_chained_df['value'].pct_change()
    mock_chained_df['FY'] = mock_chained_df['date'].dt.year
    mock_chained_df['id'] = 'GDPPI'
    mock_chained_df['title'] = 'Chained Series GDPPI'
    mock_chained_df['units'] = 'Index'
    mock_chained_df = mock_chained_df.dropna().reset_index(drop=True)

    # Configure mock_get_series to return this DataFrame when 'GDPPI' is requested
    mock_get_series.return_value = mock_chained_df
    
    index_df = escalation_instance.make_index(
        df=sample_historical_data.copy(), # Primary series data
        base_year=2022,
        outlays=(0.5, 0.5),
        fy_range=(sample_historical_data['FY'].min(), future_fy_end),
        forecast_options={'method': 'chained', 'series_id': 'GDPPI'}
    )
    
    assert not index_df.empty
    assert index_df['forecast_method_used'].iloc[0] == 'chained'
    # Assert that get_series was called for 'GDPPI'
    mock_get_series.assert_called_with('GDPPI', frequency='a', aggregation_method='eop')
    
    # Check that future rates are based on the median of the chained series' rates
    future_rates = index_df[index_df['FY'] >= future_fy_start]['rate']
    expected_chained_median_rate = mock_chained_df['rate'].median()
    assert np.allclose(future_rates, expected_chained_median_rate)

@patch('fred.ARIMA') # Patch the ARIMA class itself
def test_make_index_arima_fallback(mock_arima_class, escalation_instance, sample_historical_data):
    """Test that ARIMA forecast falls back to median if ARIMA fitting fails."""
    # Configure the mock ARIMA's fit method to raise an exception
    mock_arima_instance = Mock()
    mock_arima_instance.fit.side_effect = Exception("ARIMA fitting error")
    mock_arima_class.return_value = mock_arima_instance # When ARIMA() is called, return our mock

    future_fy_start = sample_historical_data['FY'].max() + 1
    future_fy_end = future_fy_start + 4

    index_df = escalation_instance.make_index(
        df=sample_historical_data.copy(),
        base_year=2022,
        outlays=(0.5,0.5),
        fy_range=(sample_historical_data['FY'].min(), future_fy_end),
        forecast_options={'method': 'arima', 'order': (1,1,0)}
    )
    assert not index_df.empty
    # Check if it fell back to median rates, but forecast_method_used currently remains the original.
    # This test verifies the fallback *behavior* (output rates) even if the label isn't updated.
    # A more ideal scenario would be for fred.py to update forecast_method_used to 'arima_fallback_median'.
    assert index_df['forecast_method_used'].iloc[0] == 'arima' 
    
    future_rates = index_df[index_df['FY'] >= future_fy_start]['rate']
    expected_median_rate = sample_historical_data['rate'].median()
    assert np.allclose(future_rates, expected_median_rate)
