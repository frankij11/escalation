"""
Provides a client for interacting with the FRED (Federal Reserve Economic Data) API.

This module defines the `Escalation` class, which allows users to retrieve,
search, and process economic time series data from FRED. It handles API key
management, data fetching, and some common data transformations for creating
economic indices.
"""
import logging
import os
import hashlib
import urllib.parse
from functools import lru_cache
from typing import List, Dict, Any, Optional, Tuple, Union, cast  # Added Union and cast

import pandas as pd
import requests  # Removed unused import json
import hvplot.pandas  # Retained as it might be used by consuming code or for the plotly backend setting

# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

pd.options.plotting.backend = 'plotly'

API_KEY: Optional[str] = os.environ.get("FRED_API_KEY")
if not API_KEY:
    logging.warning(
        "FRED_API_KEY environment variable not set. API calls may fail.")

# @dataclass # dataclass might be problematic with __hash__ if not handled carefully. Let's remove it for now and add __init__ explicitly.


class Escalation:
    """
    A client for fetching and processing economic data series from the FRED API.

    Attributes:
        series_id (str): Default series ID to use if none is provided.
        _api_key (Optional[str]): The API key for accessing FRED.
    """
    series_id: str = 'GDP'  # Default series ID

    def __init__(self, api_key: Optional[str] = API_KEY, series_id: str = 'GDP') -> None:
        """
        Initializes the Escalation client.

        Args:
            api_key (Optional[str]): The FRED API key. Defaults to the FRED_API_KEY
                                     environment variable.
            series_id (str): The default series ID to use. Defaults to 'GDP'.
        """
        self._api_key: Optional[str] = api_key
        self.series_id = series_id  # Set instance specific series_id
        # Stores data for the instance if needed, review usage
        self._series: pd.DataFrame = pd.DataFrame()

        if not self._api_key:
            logging.error(
                "FRED API key is not configured. Please set the FRED_API_KEY environment variable or pass it to the constructor.")
            # Consider raising an exception here if the API key is essential for all operations
            # raise ValueError("FRED API key is required.")

    def __hash__(self) -> int:
        """
        Computes a hash for the instance based on its dictionary representation.

        Note: This hash implementation relies on `str(self.__dict__)`.
        If `self.__dict__` contains mutable objects, or if the order of items
        in `__dict__` is not guaranteed across Python versions or instances,
        this could lead to unexpected behavior when instances are used in
        hash-based collections (e.g., sets, dictionary keys).
        Consider a more robust hashing strategy if instances are frequently hashed,
        perhaps based on immutable identifiers if available.
        """
        # Using only _api_key and series_id for hash, assuming these define uniqueness for hashing purposes.
        # Excludes _series which is mutable.
        h = hashlib.sha256(
            f"{self._api_key}{self.series_id}".encode('utf-8')).hexdigest()
        return hash(h)

    def get_series(self, series_ids: Union[str, List[str]],
                   url: str = 'https://api.stlouisfed.org/fred/series/observations?',
                   **kwargs: Any) -> Union[pd.DataFrame, bool]:
        """
        Retrieves one or more economic series observations from FRED.

        Args:
            series_ids (Union[str, List[str]]): A single series ID or a list of series IDs.
            url (str): The base URL for the FRED series observations endpoint.
            **kwargs: Additional parameters to pass to the API request.

        Returns:
            Union[pd.DataFrame, bool]: A DataFrame containing the series data,
                                       or False if input validation fails.
        """
        if isinstance(series_ids, str):
            series_ids = [series_ids]

        if not isinstance(series_ids, list) or not all(isinstance(sid, str) for sid in series_ids):
            logging.error("series_ids must be a string or a list of strings.")
            return False  # Return False or raise ValueError

        # Initialize an empty DataFrame to accumulate results for this specific call.
        # self._series is an instance variable that could accumulate data across calls if desired,
        # but current logic suggests df is temporary for this method's scope.
        df_list: List[pd.DataFrame] = []

        for series_id_str in series_ids:
            info: pd.DataFrame = self.get_series_info(series_id=series_id_str)
            if info.empty:
                logging.warning(
                    f"Could not retrieve info for series_id: {series_id_str}. Skipping.")
                continue

            results: pd.DataFrame = self._request_df(
                series_id=series_id_str, url=url, records_name='observations', **kwargs)

            if results.empty or 'value' not in results.columns:
                logging.warning(
                    f"No observations data returned or 'value' column missing for series_id: {series_id_str}.")
                continue

            results = results.assign(
                value=lambda x: pd.to_numeric(x.value, errors='coerce'))

            # Ensure 'units' column exists and has data
            is_percent: bool = False
            if 'units' in info.columns and not info.units.empty:
                # Accessing the first element, ensure it's safe
                first_unit = info.units.iloc[0] if isinstance(
                    info.units.iloc[0], str) else ""
                is_percent = "percent" in first_unit.lower()

            # Merge info with results
            # Ensure 'id' in info and 'series_id' in results are appropriate merge keys.
            # Assuming 'id' from series_info corresponds to 'series_id' for observations.
            merged_results = info.merge(
                results, left_on='id', right_on='series_id', suffixes=("_info", "_obs"))

            if merged_results.empty:
                logging.warning(
                    f"Merge resulted in an empty DataFrame for series_id: {series_id_str}.")
                continue

            merged_results = merged_results.assign(
                rate=lambda x: x.value /
                100 if is_percent else x.value / x.value.shift(1) - 1
            )
            df_list.append(merged_results)

        if not df_list:
            logging.warning("No data retrieved for any series_ids.")
            return pd.DataFrame()  # Return empty DataFrame if no data

        final_df = pd.concat(df_list, ignore_index=True)
        if 'date' in final_df.columns:
            final_df['date'] = pd.to_datetime(final_df['date'])
        else:
            logging.warning("'date' column not found in the final DataFrame.")

        return final_df

    def get_series_info(self, series_id: str, url: str = 'https://api.stlouisfed.org/fred/series?',
                        **kwargs: Any) -> pd.DataFrame:
        """
        Retrieves metadata for a given FRED series.

        Args:
            series_id (str): The ID of the series.
            url (str): The base URL for the FRED series endpoint.
            **kwargs: Additional parameters to pass to the API request.

        Returns:
            pd.DataFrame: A DataFrame containing the series metadata.
        """
        series_sources_df = self.get_series_sources(series_id)
        # df = self._request_df(series_id=series_id, url=url, records_name='seriess') # Original line
        # To avoid issues if get_series_sources returns empty or doesn't have 'series_id'
        main_series_info_df = self._request_df(
            series_id=series_id, url=url, records_name='seriess', **kwargs)

        if main_series_info_df.empty:
            logging.warning(f"No main series info found for {series_id}.")
            return pd.DataFrame()

        if series_sources_df.empty or 'series_id' not in series_sources_df.columns:
            logging.warning(
                f"No sources found or 'series_id' missing in sources for {series_id}. Returning main info only.")
            return main_series_info_df

        # Perform merge, ensuring 'series_id' is present in both for a meaningful merge
        # It's possible that series_id from _request_df is added as a parameter column, ensure this.
        # The _request_df adds series_id as a column if it's passed in payload.
        merged_df = pd.merge(main_series_info_df, series_sources_df,
                             on='series_id', suffixes=("", "_sources"))
        return merged_df

    def search_series(self, search_text: str, url: str = 'https://api.stlouisfed.org/fred/series/search?',
                      **kwargs: Any) -> pd.DataFrame:
        """
        Searches for FRED series based on a search string.

        Args:
            search_text (str): The text to search for.
            url (str): The base URL for the FRED series search endpoint.
            **kwargs: Additional parameters to pass to the API request.

        Returns:
            pd.DataFrame: A DataFrame containing search results.
        """
        df = self._request_df(search_text=search_text,
                              url=url, records_name='seriess', **kwargs)
        return df

    def make_index(self, series_id: Optional[Union[str, List[str]]] = None,
                   df: Optional[pd.DataFrame] = None,
                   base_year: int = 2022,
                   outlays: Tuple[float, ...] = (0.25, 0.25, 0.5),
                   # Renamed FY to fy_range for clarity
                   fy_range: Tuple[int, ...] = tuple(range(2010, 2030)),
                   forecast_method: str = 'median') -> pd.DataFrame:
        """
        Creates an economic index from series data.

        Args:
            series_id (Optional[Union[str, List[str]]]): A single series ID or list of series IDs.
                                                          If provided, data will be fetched.
            df (Optional[pd.DataFrame]): An optional DataFrame of series data. If not provided,
                                         series_id must be used to fetch data.
            base_year (int): The base year for the index (e.g., index = 100 in base_year).
            outlays (Tuple[float, ...]): A tuple of outlay percentages for calculating weighted values.
            fy_range (Tuple[int, ...]): A tuple representing the range of fiscal years.
            forecast_method (str): Method to use for forecasting future rates ('median', 'mean', etc.).

        Returns:
            pd.DataFrame: A DataFrame representing the calculated index.
        """
        # Ensure df is initialized if None is passed
        if df is None:
            df = pd.DataFrame()

        # Inner helper functions for calculations
        def _calculate_raw(input_df: pd.DataFrame, rate_col: str = 'rate') -> pd.DataFrame:
            input_df['raw'] = 1.0  # Ensure float for calculations
            for i in range(1, input_df.shape[0]):
                # Ensure rate_col exists and data is numeric
                if rate_col not in input_df.columns or not pd.api.types.is_numeric_dtype(input_df[rate_col]):
                    logging.error(
                        f"'{rate_col}' column is missing or not numeric in _calculate_raw.")
                    # Potentially return input_df or raise error
                    return input_df
                input_df.loc[i, 'raw'] = input_df.at[i-1,
                                                     'raw'] * (1 + input_df.at[i, rate_col])
            return input_df

        def _calculate_wtd(input_df: pd.DataFrame, outlay_cols: List[int]) -> pd.DataFrame:
            # Initialize with NA for better type handling
            input_df['wtd'] = pd.NA
            # Ensure outlay_cols are valid column names/indices
            valid_outlay_cols = [
                col for col in outlay_cols if col in input_df.columns]
            if len(valid_outlay_cols) != len(outlay_cols):
                logging.warning(
                    "Some outlay columns are missing in the DataFrame for _calculate_wtd.")

            # Adjusted loop range
            for i in range(input_df.shape[0] - len(valid_outlay_cols) + 1):
                if not valid_outlay_cols:  # Skip if no valid outlay columns
                    continue
                # Ensure 'raw' column exists from _calculate_raw
                if 'raw' not in input_df.columns:
                    logging.error("'raw' column missing for _calculate_wtd.")
                    return input_df

                raw_values = input_df.loc[range(
                    i, i + len(valid_outlay_cols)), 'raw'].values
                outlay_values_at_i = input_df.loc[i, valid_outlay_cols].values.astype(
                    float)  # Ensure numeric

                if len(raw_values) == len(outlay_values_at_i):
                    input_df.loc[i, 'wtd'] = (
                        raw_values * outlay_values_at_i).sum()
                else:
                    # This case should ideally not happen if logic is correct
                    logging.warning(
                        f"Shape mismatch at index {i} in _calculate_wtd.")
            return input_df

        if series_id is None and df.empty:
            logging.warning(
                "make_index requires either a series_id or a DataFrame.")
            return pd.DataFrame()

        current_df: pd.DataFrame = df.copy()  # Work on a copy

        if isinstance(series_id, str):
            fetched_data = self.get_series(
                series_id, frequency='a', aggregation_method='eop')
            if isinstance(fetched_data, pd.DataFrame):
                current_df = fetched_data
            else:  # get_series returned False or an empty DataFrame due to error
                logging.error(
                    f"Failed to fetch data for series_id: {series_id} in make_index.")
                return pd.DataFrame()
        elif isinstance(series_id, list):
            # This section recursively calls make_index. Consider if iterative approach is better.
            # Also, kwargs are passed from the outer scope, which might not be intended for recursive calls.
            index_list: List[pd.DataFrame] = []
            for s_id in series_id:
                # Explicitly pass parameters for clarity and control, avoid broad kwargs
                index_list.append(self.make_index(series_id=s_id, base_year=base_year,
                                  outlays=outlays, fy_range=fy_range, forecast_method=forecast_method))
            return pd.concat(index_list, ignore_index=True) if index_list else pd.DataFrame()

        # If multiple series IDs are implicitly in current_df (e.g., from a prior get_series call that returned multiple series)
        # This logic seems to assume 'series_id' column exists in current_df
        if 'series_id' in current_df.columns and len(current_df.series_id.unique()) > 1:
            index_list = []
            for s_id_val, group_df in current_df.groupby('series_id'):
                # Recursive call, ensure parameters are correctly passed
                index_list.append(self.make_index(df=group_df, base_year=base_year, outlays=outlays,
                                  fy_range=fy_range, forecast_method=forecast_method, series_id=str(s_id_val)))
            return pd.concat(index_list, ignore_index=True) if index_list else pd.DataFrame()

        if current_df.empty or 'rate' not in current_df.columns or 'id' not in current_df.columns or 'title' not in current_df.columns or 'units' not in current_df.columns:
            logging.error(
                "DataFrame is empty or missing required columns ('rate', 'id', 'title', 'units') for index calculation.")
            return pd.DataFrame()

        # Calculate forecast_rate based on the specified method
        if forecast_method == 'median':
            forecast_rate_val = current_df.rate.median()
        elif forecast_method == 'mean':
            forecast_rate_val = current_df.rate.mean()
        else:
            logging.warning(
                f"Unsupported forecast_method: {forecast_method}. Defaulting to median.")
            forecast_rate_val = current_df.rate.median()

        if pd.isna(forecast_rate_val):
            logging.warning(
                "Forecast rate is NA, this might affect index calculation.")
            forecast_rate_val = 0  # Default to 0 if NA to avoid errors, or handle as error

        # Prepare the base index DataFrame
        # Ensure fy_range is correctly used
        min_fy, max_fy = min(fy_range), max(fy_range) if fy_range else (
            1970, 2061)  # Default if fy_range is empty

        index_df = pd.DataFrame(dict(FY=range(min_fy, max_fy + 1)))
        index_df = index_df.assign(
            series_id=current_df.id.unique()[0],
            id=current_df.id.unique()[0],
            title=current_df.title.unique()[0],
            units=current_df.units.unique()[0],
            forecast_method=forecast_method
        )

        for i, outlay_val in enumerate(outlays):
            index_df[i] = outlay_val
        index_df['outlay_sum'] = index_df[[
            i for i, _ in enumerate(outlays)]].fillna(0).sum(axis=1)

        # Merge with actual data and apply calculations
        # Ensure 'date' column exists and is datetime for year extraction
        if 'date' not in current_df.columns or not pd.api.types.is_datetime64_any_dtype(current_df['date']):
            logging.error(
                "'date' column is missing or not datetime, cannot extract year for merging.")
            # Attempt to convert if possible, or return empty
            if 'date' in current_df.columns:
                current_df['date'] = pd.to_datetime(
                    current_df['date'], errors='coerce')
                # If all dates are NaT after conversion
                if current_df['date'].isnull().all():
                    logging.error("All dates are invalid after conversion.")
                    return pd.DataFrame()
            else:
                return pd.DataFrame()

        merged_index_df = index_df.merge(
            current_df.assign(FY=lambda x: x.date.dt.year).groupby('FY')[
                ['value', 'rate']].mean().reset_index(),
            on='FY',
            how='left'
        ).fillna({'rate': forecast_rate_val})  # Use calculated forecast_rate_val

        # Ensure columns for outlay exist before passing to _calculate_wtd
        outlay_cols_for_wtd = [i for i, _ in enumerate(
            outlays) if i in merged_index_df.columns]

        merged_index_df = (merged_index_df
                           .pipe(_calculate_raw, rate_col='rate')
                           .pipe(_calculate_wtd, outlay_cols=outlay_cols_for_wtd)
                           .assign(combo_factor=lambda x: (x.wtd / x.raw).fillna(method='ffill'))
                           .assign(base_year_val=base_year,  # Renamed to avoid conflict
                                   raw=lambda x: x.raw / x.query(f'FY=={base_year}').raw.mean() if not x.query(
                                       f'FY=={base_year}').empty and x.query(f'FY=={base_year}').raw.mean() != 0 else pd.NA,
                                   wtd=lambda x: x.combo_factor * x.raw)
                           )
        return merged_index_df

    def get_series_from_search(self, search_term: Union[str, List[str]], limit: int = 1,
                               **kwargs: Any) -> pd.DataFrame:
        """
        Fetches series data based on search terms.

        Args:
            search_term (Union[str, List[str]]): A search term or list of search terms.
            limit (int): The maximum number of search results to process per term.
            **kwargs: Additional parameters for searching and fetching series.

        Returns:
            pd.DataFrame: A DataFrame containing data for the found series.
        """
        ids: List[str] = []
        search_terms_list = self._list_of_strings(
            search_term)  # Uses the refactored helper

        for term in search_terms_list:
            try:
                search_results = self.search_series(
                    term, limit=limit, **kwargs)
                if not search_results.empty and 'id' in search_results.columns:
                    ids.extend(search_results.id.unique().tolist()
                               [:limit])  # Apply limit here
                else:
                    logging.warning(
                        f"Search for '{term}' yielded no results or 'id' column missing.")
            except Exception as e:  # Catch specific exceptions if possible
                logging.error(f"Error searching for term '{term}': {e}")

        if not ids:
            logging.warning(
                f"No series IDs found for search term(s): {search_term}")
            return pd.DataFrame()

        # Log the IDs found
        logging.info(
            f"Found series IDs: {ids} for search term(s): {search_term}")

        series_data = self.get_series(ids, **kwargs)
        if isinstance(series_data, pd.DataFrame):
            return series_data
        else:  # get_series returned False or error
            logging.error(f"Failed to get series data for IDs: {ids}")
            return pd.DataFrame()

    # Added Any for more robustness before raising TypeError
    def _list_of_strings(self, var: Union[str, List[str], Any]) -> List[str]:
        """
        Ensures the input is a list of strings.

        Args:
            var (Union[str, List[str], Any]): The input variable.

        Returns:
            List[str]: A list of strings.

        Raises:
            TypeError: If var is not a string or a list of strings.
        """
        if isinstance(var, str):
            return [var]
        if isinstance(var, list) and all(isinstance(elem, str) for elem in var):
            return var
        else:
            # Log the type issue before raising
            logging.error(
                f"Input must be a string or a list of strings, got {type(var)}.")
            raise TypeError(
                f"Provide a string or list of strings, got {type(var)}")

    def make_index_from_search(self, search_term: Union[str, List[str]], limit: int = 1,
                               base_year: int = 2022, outlays: Tuple[float, ...] = (0.25, 0.25, 0.5),
                               fy_range: Tuple[int, ...] = tuple(
                                   range(2010, 2030)),  # Renamed FY
                               forecast_method: str = 'median', **kwargs: Any) -> pd.DataFrame:  # Added **kwargs
        """
        Creates an index by first searching for series IDs then making the index.

        Args:
            search_term (Union[str, List[str]]): Search term(s) for series.
            limit (int): Max number of series to use from search.
            base_year (int): Base year for the index.
            outlays (Tuple[float, ...]): Outlay percentages.
            fy_range (Tuple[int, ...]): Fiscal year range.
            forecast_method (str): Forecasting method.
            **kwargs: Additional arguments for get_series_from_search and make_index.

        Returns:
            pd.DataFrame: The resulting index DataFrame.
        """
        # kwargs from locals() is not needed if parameters are passed explicitly.
        # The original kwargs were: kwargs = locals(); del kwargs['self']; del kwargs['search_term']; del kwargs['limit']
        # This is error-prone. Better to explicitly pass what's needed.

        df_list: List[pd.DataFrame] = []
        search_terms_list = self._list_of_strings(search_term)

        for term in search_terms_list:
            try:
                # Pass only relevant kwargs to get_series_from_search
                # Assuming get_series_from_search does not need base_year, outlays etc.
                # And make_index does not need limit.
                # This requires careful separation of concerns or passing all **kwargs.
                # For now, let's assume get_series_from_search uses its own relevant kwargs
                # and make_index uses its own. The common ones like 'frequency' might be in **kwargs.

                # Get series IDs first
                series_data_for_term = self.get_series_from_search(
                    term, limit=limit, **kwargs)  # Pass kwargs here

                if not series_data_for_term.empty and 'id' in series_data_for_term.columns:
                    unique_ids_for_term = series_data_for_term['id'].unique(
                    ).tolist()
                    if unique_ids_for_term:
                        # Call make_index with these IDs
                        # Pass make_index specific params and also **kwargs if they are relevant for it too
                        index_df = self.make_index(series_id=unique_ids_for_term,
                                                   base_year=base_year,
                                                   outlays=outlays,
                                                   fy_range=fy_range,
                                                   forecast_method=forecast_method,
                                                   df=series_data_for_term,  # Pass the fetched data directly
                                                   **kwargs)  # Pass kwargs here
                        if not index_df.empty:
                            df_list.append(index_df)
                    else:
                        logging.warning(
                            f"No unique series IDs found from search data for term: {term}")
                else:
                    logging.warning(
                        f"Search for series data for term '{term}' returned empty or no 'id' column.")

            except Exception as e:  # More specific exception if possible
                logging.error(
                    f"Failed to make index for search term '{term}': {e}")
                # Optionally re-raise or continue

        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

    def get_series_release(self, series_id: str, url: str = 'https://api.stlouisfed.org/fred/series/release?',
                           **kwargs: Any) -> pd.DataFrame:
        """
        Gets release information for a FRED series.

        Args:
            series_id (str): The ID of the series.
            url (str): The FRED API URL for series release information.
            **kwargs: Additional parameters for the API request.

        Returns:
            pd.DataFrame: DataFrame with release information.
        """
        # The _request_df will add series_id as a column if it's in payload.
        # Ensure series_id is part of the payload for _request_df if the API needs it.
        df = self._request_df(url=url, series_id=series_id,
                              records_name='releases', **kwargs)
        return df

    def get_series_sources(self, series_id: str, url: str = 'https://api.stlouisfed.org/fred/release/sources?',
                           **kwargs: Any) -> pd.DataFrame:
        """
        Gets sources for a FRED series release.

        Args:
            series_id (str): The ID of the series to get sources for.
            url (str): The FRED API URL for release sources.
            **kwargs: Additional parameters for the API request.

        Returns:
            pd.DataFrame: DataFrame with source information.
        """
        release_df = self.get_series_release(
            series_id, **kwargs)  # Pass kwargs
        if release_df.empty or 'id' not in release_df.columns:
            logging.warning(
                f"No release information found for series_id: {series_id}, cannot get sources.")
            return pd.DataFrame()

        # Assuming the first release is the relevant one if multiple are returned
        release_id = release_df['id'].unique()[0]

        # Rename columns for clarity before fetching sources
        release_df = release_df.rename(
            columns={'id': 'release_id', 'name': 'release_name', 'link': 'release_link'})

        sources_df = self._request_df(
            url=url, release_id=release_id, records_name='sources', **kwargs)  # Pass kwargs

        if sources_df.empty:
            logging.warning(
                f"No sources found for release_id: {release_id} (from series_id: {series_id}).")
            # Return release_df with a note that sources are missing, or an empty df
            # For consistency, let's return an empty df if sources are the primary goal and not found.
            # However, the original code appends to release_df, so let's try to follow that structure.
            # If sources_df is empty, the join will be empty for source columns.
            # return pd.DataFrame()
            # Let's assign empty strings if sources are not found to maintain structure.
            release_df = release_df.assign(source="", source_links="")
            return release_df

        # Aggregate source names and links
        # Ensure 'name' and 'link' columns exist in sources_df
        source_names = ", ".join(
            sources_df.name.unique()) if 'name' in sources_df.columns else ""
        source_links = ", ".join(
            sources_df.link.unique()) if 'link' in sources_df.columns else ""

        # Assign aggregated sources to the release_df.
        # This assumes release_df has one row per original release_id. If multiple, this might duplicate info.
        # The original code returns df.assign(...) which implies it's adding to the release_df structure.
        final_df = release_df.assign(
            source=source_names, source_links=source_links)
        # Ensure series_id is part of the final DataFrame for merging upstream.
        final_df['series_id'] = series_id
        return final_df

    @lru_cache(maxsize=128)  # Increased cache size slightly
    def _request_json(self, url: str, **payload: Any) -> Dict[str, Any]:
        """
        Internal method to make a GET request and return JSON data.
        Uses LRU cache.

        Args:
            url (str): The request URL.
            **payload (Any): Query parameters for the request.
                             Includes 'api_key' and 'file_type' by default.

        Returns:
            Dict[str, Any]: The JSON response as a dictionary.

        Raises:
            requests.exceptions.RequestException: For issues like network errors or HTTP errors.
            ValueError: If API key is missing.
        """
        if not self._api_key:
            logging.error("API key not available for _request_json.")
            raise ValueError("API key is required for FRED requests.")

        # Ensure common parameters are set correctly
        payload_with_api = {
            'api_key': self._api_key,
            'file_type': 'json',
            **payload  # Overwrite defaults if present in payload
        }

        # Filter out None values from payload to prevent issues with urlencode
        # For example, if series_id=None is passed, it might become 'series_id=None' in the URL
        # FRED API might be okay with it, but it's cleaner not to send.
        # However, some params might be legitimately empty strings.
        # Let's assume FRED API handles params like 'series_id=' if an empty string is intended.
        # The main concern is 'None' being stringified.
        # urllib.parse.urlencode handles dicts well, but let's be explicit.
        encoded_payload = urllib.parse.urlencode(
            {k: v for k, v in payload_with_api.items() if v is not None})

        full_url = f"{url}?{encoded_payload}" if '?' in url else f"{url}&{encoded_payload}"
        # The original code did url + '&api_key=' + ... which might create issues if url already has '?'
        # Corrected logic: if '?' not in url, use '?', else use '&'.
        # Actually, requests library handles params dict directly, which is safer.
        # Let's revert to using requests' `params` argument.

        try:
            # Using requests' `params` argument is cleaner and safer for URL construction
            response = requests.get(url, params=payload_with_api)
            response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
            # Cast for type safety
            return cast(Dict[str, Any], response.json())
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred: {e} - URL: {response.url}")
            # Optionally return a more structured error or empty dict
            # For now, re-raise to indicate failure
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e} - URL: {url}")
            raise
        except ValueError as e:  # Handles JSON decoding errors
            logging.error(f"JSON decoding error: {e} - URL: {url}")
            raise

    # Removed lru_cache from _request_df as it might cache DataFrames with mutable content,
    # or if params in payload are complex. Caching at _request_json level is safer for raw responses.
    # If _request_df results need caching, ensure DataFrame immutability or hashability.
    def _request_df(self, url: str, records_name: str, **payload: Any) -> pd.DataFrame:
        """
        Internal method to request data and parse it into a DataFrame.
        It prepends payload items as columns to the resulting DataFrame for context.

        Args:
            url (str): The API endpoint URL.
            records_name (str): The key in the JSON response that contains the list of records.
            **payload (Any): Query parameters for the request.

        Returns:
            pd.DataFrame: A DataFrame created from the records. Returns an empty
                          DataFrame if the request fails or records_name is not found.
        """
        try:
            json_response = self._request_json(url, **payload)
            if records_name not in json_response:
                logging.warning(
                    f"'{records_name}' not found in JSON response from {url}. Payload: {payload}")
                return pd.DataFrame()

            records = json_response.get(records_name)
            # FRED usually returns a list for 'observations', 'seriess', etc.
            if not isinstance(records, list):
                logging.warning(
                    f"Expected a list for '{records_name}', but got {type(records)}. URL: {url}")
                return pd.DataFrame()

            df = pd.DataFrame.from_records(records)

            # Add payload items as columns for context, avoid overwriting existing columns
            for key, item in payload.items():
                col_name = key
                if col_name in df.columns:
                    col_name = f"{key}_param"  # Suffix to avoid collision
                df.insert(0, column=col_name, value=item)
            return df
        except (requests.exceptions.RequestException, ValueError) as e:
            # Errors are logged in _request_json, but we can add context here
            logging.error(
                f"Failed to create DataFrame from request to {url} (records: {records_name}): {e}")
            return pd.DataFrame()  # Return empty DataFrame on error
        except Exception as e:  # Catch any other unexpected errors during DataFrame creation
            logging.error(
                f"Unexpected error creating DataFrame for {url} (records: {records_name}): {e}")
            return pd.DataFrame()
