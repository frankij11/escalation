import pytest
import pandas as pd
import datetime
from typing import List, Dict, Any, Tuple, Optional

# Import the helper functions to be tested
from escalation import _build_config_data_dict, _build_excel_metadata_dict

# --- Tests for _build_config_data_dict ---

def test_build_config_data_dict_median_forecast():
    """Test _build_config_data_dict with median forecast."""
    config = _build_config_data_dict(
        index_name_val="MedianTest",
        series_ids_val=["GDP", "CPI"],
        outlays_df_val=pd.DataFrame([{'0': 0.5, '1': 0.5, 'sum': 1.0}]),
        base_year_val=2020,
        forecast_method_val="median",
        arima_p_val=1, arima_d_val=1, arima_q_val=0, # Irrelevant for median
        es_trend_val=None, es_seasonal_val=None, es_seasonal_periods_val=None, # Irrelevant
        chained_series_id_val="", # Irrelevant
        fys_slider_val=(2010, 2030)
    )
    assert config["indexName"] == "MedianTest"
    assert config["baseYear"] == 2020
    assert len(config["seriesComponents"]) == 2
    assert config["seriesComponents"][0]['id'] == "GDP"
    assert config["forecastOptions"]["method"] == "median"
    assert "createdAt" in config
    assert config["version"] == "1.0"
    assert config["fiscalYearRangeForPlot"] == (2010, 2030)
    assert isinstance(config["outlays"], list)
    assert len(config["outlays"]) == 1
    assert config["outlays"][0]['0'] == 0.5

def test_build_config_data_dict_arima_forecast():
    """Test _build_config_data_dict with ARIMA forecast."""
    config = _build_config_data_dict(
        index_name_val="ArimaTest",
        series_ids_val=["INDPRO"],
        outlays_df_val=pd.DataFrame([{'0': 1.0, 'sum': 1.0}]),
        base_year_val=2019,
        forecast_method_val="arima",
        arima_p_val=2, arima_d_val=1, arima_q_val=2,
        es_trend_val=None, es_seasonal_val=None, es_seasonal_periods_val=None,
        chained_series_id_val="",
        fys_slider_val=(2000, 2025)
    )
    assert config["indexName"] == "ArimaTest"
    assert config["forecastOptions"]["method"] == "arima"
    assert config["forecastOptions"]["order"] == (2, 1, 2)

def test_build_config_data_dict_exponential_smoothing_forecast():
    """Test _build_config_data_dict with Exponential Smoothing forecast."""
    config = _build_config_data_dict(
        index_name_val="ESTest",
        series_ids_val=["PCE"],
        outlays_df_val=pd.DataFrame(), # Empty outlays
        base_year_val=2021,
        forecast_method_val="exponential_smoothing",
        arima_p_val=1, arima_d_val=1, arima_q_val=0,
        es_trend_val="add", es_seasonal_val="mul", es_seasonal_periods_val=12,
        chained_series_id_val="",
        fys_slider_val=(2015, 2035)
    )
    assert config["indexName"] == "ESTest"
    assert config["forecastOptions"]["method"] == "exponential_smoothing"
    assert config["forecastOptions"]["trend"] == "add"
    assert config["forecastOptions"]["seasonal"] == "mul"
    assert config["forecastOptions"]["seasonal_periods"] == 12
    assert config["outlays"] == []

def test_build_config_data_dict_chained_forecast():
    """Test _build_config_data_dict with chained forecast."""
    config = _build_config_data_dict(
        index_name_val="ChainedTest",
        series_ids_val=["GDP"],
        outlays_df_val=pd.DataFrame([{'0': 0.7, '1': 0.3, 'sum':1.0}]),
        base_year_val=2022,
        forecast_method_val="chained",
        arima_p_val=1, arima_d_val=1, arima_q_val=0,
        es_trend_val=None, es_seasonal_val=None, es_seasonal_periods_val=None,
        chained_series_id_val="GDPPI",
        fys_slider_val=(2018, 2028)
    )
    assert config["indexName"] == "ChainedTest"
    assert config["forecastOptions"]["method"] == "chained"
    assert config["forecastOptions"]["series_id"] == "GDPPI"
    assert config["forecastOptions"]["chain_type"] == "rate"


# --- Tests for _build_excel_metadata_dict ---

@pytest.fixture
def sample_main_df_for_meta() -> pd.DataFrame:
    """Provides a sample main_data_df for metadata extraction."""
    return pd.DataFrame({
        'series_id': ['GDP', 'CPIAUCNS', 'GDP'], # Test duplicate series_id
        'title': ['Gross Domestic Product', 'Consumer Price Index', 'Gross Domestic Product']
    })

@pytest.fixture
def sample_outlays_df_for_meta() -> pd.DataFrame:
    """Provides a sample outlays_df for metadata."""
    return pd.DataFrame([{'0': 0.6, '1': 0.4, 'sum': 1.0}])


def test_build_excel_metadata_dict_all_fields(sample_main_df_for_meta, sample_outlays_df_for_meta):
    """Test _build_excel_metadata_dict with all fields populated and ARIMA forecast."""
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metadata = _build_excel_metadata_dict(
        export_timestamp_val=timestamp,
        index_name_val="FullReport",
        base_year_val=2020,
        fys_slider_val=(2010, 2030),
        active_series_ids_val=["GDP", "CPIAUCNS"],
        main_data_df_for_meta=sample_main_df_for_meta,
        outlays_df_val=sample_outlays_df_for_meta,
        forecast_method_val="arima",
        arima_p_val=1, arima_d_val=1, arima_q_val=1,
        es_trend_val=None, es_seasonal_val=None, es_seasonal_periods_val=None,
        chained_series_id_val=""
    )
    assert metadata["Report Generated At"] == timestamp
    assert metadata["Index Configuration Name"] == "FullReport"
    assert metadata["Base Year"] == 2020
    assert "Gross Domestic Product (GDP)" in metadata["Component Series"]
    assert "Consumer Price Index (CPIAUCNS)" in metadata["Component Series"]
    assert isinstance(metadata["Outlay Weights"], list)
    assert metadata["Forecast Method"] == "arima"
    assert metadata["ARIMA Order (p,d,q)"] == (1, 1, 1)

def test_build_excel_metadata_dict_minimal_fields_median_forecast(sample_outlays_df_for_meta):
    """Test _build_excel_metadata_dict with minimal fields and median forecast."""
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    # Simulate main_data_df not having title, or active_series_ids being the source
    minimal_main_df = pd.DataFrame({'series_id': ['AAA', 'BBB']}) 
    
    metadata = _build_excel_metadata_dict(
        export_timestamp_val=timestamp,
        index_name_val="", # Empty index name
        base_year_val=2022,
        fys_slider_val=(2015, 2025),
        active_series_ids_val=["AAA", "BBB"],
        main_data_df_for_meta=minimal_main_df, 
        outlays_df_val=sample_outlays_df_for_meta,
        forecast_method_val="median",
        arima_p_val=0, arima_d_val=0, arima_q_val=0, # Should not appear for median
        es_trend_val=None, es_seasonal_val=None, es_seasonal_periods_val=None,
        chained_series_id_val=""
    )
    assert metadata["Index Configuration Name"] == "N/A (Not Saved/Named)"
    assert "AAA" in metadata["Component Series"] # Falls back to IDs
    assert "BBB" in metadata["Component Series"]
    assert metadata["Forecast Method"] == "median"
    assert "ARIMA Order (p,d,q)" not in metadata
    assert "Exponential Smoothing Trend" not in metadata
    assert "Chained Series ID" not in metadata

def test_build_excel_metadata_dict_exp_smoothing(sample_main_df_for_meta, sample_outlays_df_for_meta):
    """Test _build_excel_metadata_dict for exponential smoothing."""
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metadata = _build_excel_metadata_dict(
        export_timestamp_val=timestamp,
        index_name_val="ExpSmoothReport",
        base_year_val=2021,
        fys_slider_val=(2012, 2032),
        active_series_ids_val=["GDP"],
        main_data_df_for_meta=sample_main_df_for_meta,
        outlays_df_val=sample_outlays_df_for_meta,
        forecast_method_val="exponential_smoothing",
        arima_p_val=0, arima_d_val=0, arima_q_val=0,
        es_trend_val="mul", es_seasonal_val="add", es_seasonal_periods_val=4,
        chained_series_id_val=""
    )
    assert metadata["Forecast Method"] == "exponential_smoothing"
    assert metadata["Exponential Smoothing Trend"] == "mul"
    assert metadata["Exponential Smoothing Seasonal"] == "add"
    assert metadata["Exponential Smoothing Seasonal Periods"] == 4

def test_build_excel_metadata_dict_chained(sample_main_df_for_meta, sample_outlays_df_for_meta):
    """Test _build_excel_metadata_dict for chained forecast."""
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metadata = _build_excel_metadata_dict(
        export_timestamp_val=timestamp,
        index_name_val="ChainedReport",
        base_year_val=2023,
        fys_slider_val=(2013, 2033),
        active_series_ids_val=["PCE"], # Different series
        main_data_df_for_meta=pd.DataFrame({'series_id': ['PCE'], 'title': ['Personal Consumption Expenditures']}),
        outlays_df_val=sample_outlays_df_for_meta,
        forecast_method_val="chained",
        arima_p_val=0, arima_d_val=0, arima_q_val=0,
        es_trend_val=None, es_seasonal_val=None, es_seasonal_periods_val=None,
        chained_series_id_val="GDPDEF"
    )
    assert metadata["Forecast Method"] == "chained"
    assert metadata["Chained Series ID"] == "GDPDEF"
    assert "Personal Consumption Expenditures (PCE)" in metadata["Component Series"]
