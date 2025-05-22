"""
Panel application for analyzing and visualizing economic escalation indices from FRED data.

This application allows users to:
- Search for economic series from FRED.
- Generate custom economic indices based on selected series, outlays, and base years.
- View data in tabular and Perspective (pivot table) formats.
- Visualize indices using interactive plots.
"""
import logging
import json
import datetime
from io import BytesIO
# Added basic typing imports, will add more specific ones like pd.DataFrame later
from typing import Any, List, Union, Tuple, Dict, Optional # Added Optional

import pandas as pd
import panel as pn
from panel.widgets import Tabulator, MultiChoice, Select, TextInput, IntInput, Button, IntRangeSlider, FileDownload, FileInput # Added FileDownload, FileInput
from panel.pane import Perspective, Plotly, Markdown
from panel.layout import Card, Row, Tabs, Column 
from panel.template import FastListTemplate 
# hvplot.pandas and plotly.express are not directly used.
# The plotly backend for pandas plotting is enabled via pd.options.
import param # For event type hint if needed

from fred import Escalation

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

pd.options.plotting.backend = 'plotly'
pn.extension('plotly', 'tabulator', 'perspective', sizing_mode='stretch_width')


# Initialize FRED Escalation client
# This instance is shared across the application.
# Consider if a different scoping or dependency injection might be needed for more complex scenarios.
esc = Escalation()

# --- Helper function for renaming columns ---


def _rename_numeric_cols_to_str(df: pd.DataFrame, num_cols: int = 10) -> pd.DataFrame:
    """
    Renames numeric column names (0 to num_cols-1) to their string representations.
    This is often required for compatibility with JavaScript-based table/grid libraries
    used by Panel components like Tabulator and Perspective, which might not
    handle integer column names correctly.

    Args:
        df (pd.DataFrame): The DataFrame to rename.
        num_cols (int): The number of numeric columns (e.g., 0 to 9 if num_cols=10)
                        to check for and rename.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    # Identify numeric columns that are likely to be the ones needing renaming (0, 1, 2...).
    # This is safer than blindly renaming all columns that are integers.
    cols_to_rename = {i: str(i) for i in range(num_cols) if i in df.columns}
    if cols_to_rename:
        return df.rename(columns=cols_to_rename)
    return df

# --- Data and Widget Setup ---


# Initial series data loaded on application start
# Fetches data for a predefined set of series to populate the view initially.
# Updated to pass forecast_options (defaulting to median)
try:
    initial_series_data = esc.make_index_from_search(
        search_term=["Gross Domestic Product Deflator", "CPI", "PPI"],
        limit=1,
        forecast_options={'method': 'median'} # Default for initial load
    ).drop_duplicates()
except Exception as e:
    logger.error(f"Failed to load initial series data: {e}", exc_info=True)
    initial_series_data = pd.DataFrame()  # Start with empty on error

# Rename columns for display in Tabulator and Perspective
processed_initial_series = _rename_numeric_cols_to_str(initial_series_data)

df_widget = Tabulator(
    processed_initial_series, pagination='remote', page_size=10) # Used specific import
indices = Perspective( # Used specific import
    processed_initial_series, toggle_config=False, theme="material-dark")

# Widgets for user inputs
series_options: List[str] = []
if 'id' in processed_initial_series.columns:
    series_options = processed_initial_series.id.unique().tolist()
elif not processed_initial_series.empty:
    logger.warning(
        "'id' column not found in initial_series_data. MultiChoice for series_ids will be empty.")

series_ids_widget = MultiChoice( # Used specific import
    name="Select Series for Index", options=series_options)
base_year_widget = Select( # Used specific import
    name="Base Year", value=2023, options=list(range(1970, 2061)))

outlay_columns = ['sum'] + [str(i) for i in range(10)]
initial_outlay_data = [[1, 0.75, 0.25] + [0.0] * 8]
outlays_widget = Tabulator( # Used specific import
    name="Outlay Profile (Yearly %)",
    value=pd.DataFrame(columns=outlay_columns, data=initial_outlay_data),
    layout='fit_data',
    disabled=True # Kept disabled as per original; can be enabled if editing is desired
)

fys_slider = IntRangeSlider( # Used specific import
    name="Fiscal Year Range for Plot", start=1970, end=2060, value=(2000, 2025))

search_input = TextInput( # Used specific import
    name="Search FRED Series (e.g., 'GDPDEF', 'CPIAUCSL')")
search_limit_input = IntInput( # Used specific import
    name="Limit Search Results", start=1, value=1, end=5)
search_button = Button( # Used specific import
    name="Search and Add Series to Table")


# --- Forecasting Widgets ---
forecast_method_select = Select( # Used specific import
    name="Select Forecast Method",
    options=['median', 'mean', 'arima', 'exponential_smoothing', 'chained'],
    value='median'
)

# ARIMA options
arima_p_input = IntInput(name='ARIMA P (AutoRegressive order)', value=1, start=0, end=10, width=150)
arima_d_input = IntInput(name='ARIMA D (Differencing order)', value=1, start=0, end=5, width=150)
arima_q_input = IntInput(name='ARIMA Q (Moving Average order)', value=0, start=0, end=10, width=150)

# Exponential Smoothing options
es_trend_select = Select(name='Trend Component', options=['add', 'mul', None], value='add', width=150)
es_seasonal_select = Select(name='Seasonal Component', options=['add', 'mul', None], value=None, width=150)
es_seasonal_periods_input = IntInput(name='Seasonal Periods', value=4, start=0, width=150) # e.g., 4 for quarterly

# Chained series options
chained_series_id_input = TextInput(name='Chained Series ID (e.g., GDPPI)', placeholder='Enter FRED Series ID', width=300)
# chained_type_select is simplified to 'rate' in fred.py, so no widget for it now.

# Dynamic UI container for forecast options
forecast_options_ui = Column(sizing_mode='stretch_width') # Used specific import


# --- Callback Functions ---

def update_forecast_options_ui(event: Any) -> None: 
    """
    Updates the visibility of forecast-specific input widgets based on the
    selected forecast method in `forecast_method_select`.
    """
    selected_method = forecast_method_select.value
    forecast_options_ui.clear() # Clear previous options

    if selected_method == 'arima':
        forecast_options_ui.extend([
            pn.Row(arima_p_input, arima_d_input, arima_q_input) # Using pn.Row for better layout
        ])
    elif selected_method == 'exponential_smoothing':
        forecast_options_ui.extend([
            pn.Row(es_trend_select, es_seasonal_select),
            es_seasonal_periods_input
        ])
    elif selected_method == 'chained':
        forecast_options_ui.append(chained_series_id_input)
    else: # 'median', 'mean', or others that don't require specific UI options
        forecast_options_ui.append(
            Markdown("No specific options required for this method.") # Used specific import
        )

# Link callback to forecast_method_select value changes
forecast_method_select.param.watch(update_forecast_options_ui, 'value')
# Call it once at startup to initialize the UI based on the default method
update_forecast_options_ui(None) # Pass None or a dummy event for initial call


def add_series_data(event: Any) -> None:
    """
    Callback triggered by the search button (`search_button`).
    Fetches series data from FRED based on `search_input` and `search_limit_input`,
    using the currently selected forecasting options, then updates the
    Tabulator (`df_widget`) and Perspective (`indices`) panes.
    The search input field is cleared after the search.

    Args:
        event (Any): The event object from the button click (not used directly).
    """
    template.loading = True
    try:
        logger.info(
            f"Searching for series: {search_input.value} with limit: {search_limit_input.value} "
            f"and forecast: {forecast_method_select.value}"
        )
        if not search_input.value:
            logger.warning("Search input is empty. Skipping search.")
            pn.state.notifications.warning("Search term cannot be empty.", duration=3000)
            return

        # Gather current forecast options for make_index_from_search
        current_forecast_options: Dict[str, Any] = {'method': forecast_method_select.value}
        selected_method = forecast_method_select.value

        if selected_method == 'arima':
            if not (arima_p_input.value >= 0 and arima_d_input.value >= 0 and arima_q_input.value >= 0):
                pn.state.notifications.error("ARIMA P, D, and Q orders must be non-negative.", duration=4000)
                return
            current_forecast_options['order'] = (arima_p_input.value, arima_d_input.value, arima_q_input.value)
        elif selected_method == 'exponential_smoothing':
            if es_seasonal_periods_input.value < 0:
                pn.state.notifications.error("Exponential Smoothing seasonal periods must be non-negative.", duration=4000)
                return
            current_forecast_options['trend'] = es_trend_select.value
            current_forecast_options['seasonal'] = es_seasonal_select.value
            current_forecast_options['seasonal_periods'] = es_seasonal_periods_input.value if es_seasonal_periods_input.value > 0 else None
        elif selected_method == 'chained':
            if not chained_series_id_input.value:
                pn.state.notifications.error("Chained Series ID must be provided for 'chained' forecast when adding series.", duration=4000)
                return
            current_forecast_options['series_id'] = chained_series_id_input.value
            current_forecast_options['chain_type'] = 'rate' # Simplified in fred.py

        # Call make_index_from_search with the gathered forecast_options
        new_series_df = esc.make_index_from_search(
            search_term=search_input.value, 
            limit=search_limit_input.value,
            forecast_options=current_forecast_options
        )
        if new_series_df.empty:
            logger.info(f"No data found for search: {search_input.value}")
            pn.state.notifications.info(
                f"No data found for '{search_input.value}'.", duration=3000)
            return

        logger.debug(
            f"Original columns from search: {new_series_df.columns.tolist()}")
        # Rename numeric columns to string representations for Perspective/Tabulator compatibility
        processed_new_series_df = _rename_numeric_cols_to_str(new_series_df)
        logger.debug(
            f"Processed columns for streaming: {processed_new_series_df.columns.tolist()}")

        # Stream to Tabulator
        df_widget.stream(processed_new_series_df)

        # Update Perspective by concatenating new data with existing data
        # and dropping potential duplicates if same series/FY is added.
        if indices.object is None or indices.object.empty:
            indices.object = processed_new_series_df
        elif isinstance(indices.object, pd.DataFrame):
            indices.object = pd.concat(
                [indices.object, processed_new_series_df],
                ignore_index=True
            ).drop_duplicates(subset=['id', 'FY']) # Assuming 'id' and 'FY' for uniqueness
        else: # Should not happen if initialized correctly
            logger.warning("indices.object is not a DataFrame, replacing with new data.")
            indices.object = processed_new_series_df
            
        # Store original search term before clearing
        searched_term = search_input.value
        search_input.value = ''  # Clear search input
        pn.state.notifications.success(
            f"Added data for '{searched_term}'.", duration=3000)

    except Exception as e:
        logger.error(
            f"Error during series search or streaming: {e}", exc_info=True)
        pn.state.notifications.error(
            f"Error adding series: {e}", duration=5000)
    finally:
        template.loading = False


search_button.on_click(add_series_data)


@pn.depends(
    series_ids=series_ids_widget, 
    outlays_df=outlays_widget, 
    base_year=base_year_widget,
    # Forecast widget dependencies:
    forecast_method=forecast_method_select,
    arima_p=arima_p_input,
    arima_d=arima_d_input,
    arima_q=arima_q_input,
    es_trend=es_trend_select,
    es_seasonal=es_seasonal_select,
    es_seasonal_periods=es_seasonal_periods_input,
    chained_series_id=chained_series_id_input,
    watch=True # Ensure it reacts to changes in any of these
)
def update_main_indices(
    series_ids: List[str], 
    outlays_df: pd.DataFrame, 
    base_year: int,
    # Forecast widget values passed by decorator:
    forecast_method: str,
    arima_p: int, arima_d: int, arima_q: int,
    es_trend: str, es_seasonal: Optional[str], es_seasonal_periods: Optional[int],
    chained_series_id: str
) -> None:
    """
    Callback triggered by changes in various input widgets (series, outlays, base year, forecast options).
    This function orchestrates the creation of forecast_options and calls `esc.make_index`.
    the creation of forecast_options and calls `esc.make_index`.

    Args:
        series_ids (List[str]): Selected series IDs.
        outlays_df (pd.DataFrame): Outlay profile DataFrame.
        base_year (int): Selected base year.
        # Forecast related parameters are now read directly from their widgets inside the function
    """
    logger.info(f"Updating indices for series: {series_ids}, base_year: {base_year}, forecast_method: {forecast_method}") # forecast_method from decorator
    logger.debug(f"Outlays DataFrame for update:\n{outlays_df}")
    
    template.loading = True
    try:
        if not series_ids:
            logger.warning("No series IDs selected. Cannot update index.")
            pn.state.notifications.warning("Please select at least one series ID.", duration=3000)
            return

        # Construct forecast_options dictionary using values passed by @pn.depends
        current_forecast_options: Dict[str, Any] = {'method': forecast_method}
        
        if forecast_method == 'arima':
            if not (arima_p >= 0 and arima_d >= 0 and arima_q >= 0):
                pn.state.notifications.error("ARIMA P, D, and Q orders must be non-negative.", duration=4000)
                return
            current_forecast_options['order'] = (arima_p, arima_d, arima_q)
        elif forecast_method == 'exponential_smoothing':
            if es_seasonal_periods is not None and es_seasonal_periods < 0:
                pn.state.notifications.error("Exponential Smoothing seasonal periods must be non-negative.", duration=4000)
                return
            current_forecast_options['trend'] = es_trend
            current_forecast_options['seasonal'] = es_seasonal
            current_forecast_options['seasonal_periods'] = es_seasonal_periods if es_seasonal_periods is not None and es_seasonal_periods > 0 else None
        elif forecast_method == 'chained':
            if not chained_series_id:
                pn.state.notifications.error("Chained Series ID cannot be empty for 'chained' forecast method.", duration=4000)
                return 
            current_forecast_options['series_id'] = chained_series_id
            current_forecast_options['chain_type'] = 'rate'

        if outlays_df.empty:
            logger.error("Outlays DataFrame is empty.")
            pn.state.notifications.error("Outlay profile is missing.", duration=3000)
            return
            
        outlay_values_str_cols = [str(i) for i in range(10) if str(i) in outlays_df.columns]
        if not outlay_values_str_cols:
            logger.error("No valid outlay columns found in outlays_df.")
            pn.state.notifications.error("Invalid outlay profile structure.", duration=3000)
            return
        
        outlay_profile = tuple(outlays_df.iloc[0][outlay_values_str_cols].astype(float))
        logger.debug(f"Extracted outlay profile: {outlay_profile}")

        # Call make_index with the constructed forecast_options
        updated_df = esc.make_index(
            series_ids=series_ids, 
            outlays=outlay_profile, 
            base_year=base_year,
            forecast_options=current_forecast_options # Pass the new options
        )
        
        if updated_df.empty:
            logger.warning(f"make_index returned an empty DataFrame for series: {series_ids} and options: {current_forecast_options}")
            pn.state.notifications.info("No data returned for the selected index parameters.", duration=3000)
            # df_widget.value = pd.DataFrame() # Clear table
            # indices.object = pd.DataFrame() # Clear perspective
            # return # Keep existing data or clear? For now, let's not clear.

        processed_updated_df = _rename_numeric_cols_to_str(updated_df)

        df_widget.object = processed_updated_df
        indices.object = processed_updated_df # Perspective usually needs full object replacement
        
        # User notification about the method used
        method_used_for_notification = processed_updated_df['forecast_method_used'].iloc[0] if not processed_updated_df.empty and 'forecast_method_used' in processed_updated_df.columns else current_forecast_options.get('method', 'N/A')
        pn.state.notifications.success(
            f"Indices updated using '{method_used_for_notification}' forecast method.",
            duration=3000
        )
        logger.info(f"Successfully updated main index displays using {method_used_for_notification} forecast.")

    except Exception as e:
        logger.error(f"Error updating main indices: {e}", exc_info=True)
        pn.state.notifications.error(
            f"Error calculating index: {e}", duration=5000)
    finally:
        template.loading = False


@pn.depends(data_for_graph=df_widget, fiscal_year_range=fys_slider)
def graph_dynamic_indices(data_for_graph: pd.DataFrame, fiscal_year_range: Tuple[int, int]) -> Row:
    """
    Callback triggered by changes in `df_widget` (data source) or `fys_slider` (fiscal year range).
    Generates and returns a Panel Row containing cards for 'rate' and 'raw' index plots.
    Plots are filtered by the selected fiscal year range.

    Args:
        data_for_graph (pd.DataFrame): DataFrame from `df_widget.value`.
        fiscal_year_range (Tuple[int, int]): (start_year, end_year) from `fys_slider.value`.

    Returns:
        pn.Row: A Panel Row with plots. Returns an empty Row with Markdown if data is unsuitable.
    """
    if data_for_graph.empty or 'FY' not in data_for_graph.columns:
        logger.warning(
            "Data for graphing is empty or missing 'FY' column. Returning empty graph.")
        return Row(Markdown("### No data to display graphs."))

    # Filter data based on the fiscal year range slider
    fy_start, fy_end = fiscal_year_range
    filtered_df = data_for_graph[(data_for_graph['FY'] >= fy_start) & (
        data_for_graph['FY'] <= fy_end)]

    if filtered_df.empty:
        logger.info("No data within the selected fiscal year range to plot.")
        return Row(Markdown(f"### No data available for years {fy_start}-{fy_end}."))

    # Create plots using the DataFrame's .plot() method, relying on plotly backend
    plots_to_render: List[Card] = []

    if 'rate' in filtered_df.columns and 'series_id' in filtered_df.columns:
        try:
            rate_plot_fig = filtered_df.plot(x='FY', y='rate', color='series_id',
                                             title="Yearly Rate by Series",
                                             labels={'FY': 'Fiscal Year', 'rate': 'Rate', 'series_id': 'Series ID'})
            # Ensure it's a Plotly figure with a layout attribute
            if hasattr(rate_plot_fig, 'layout'):
                rate_plot_fig.layout.autosize = True
            plots_to_render.append(
                Card(Plotly(rate_plot_fig, config={'responsive': True}), title="Rate Plot"))
        except Exception as e:
            logger.error(f"Error generating rate plot: {e}", exc_info=True)
            plots_to_render.append(
                Card(f"Could not generate rate plot: {e}", title="Rate Plot Error"))
    else:
        logger.warning(
            "Skipping rate plot: 'rate' or 'series_id' column missing.")

    if 'raw' in filtered_df.columns and 'series_id' in filtered_df.columns:
        try:
            raw_plot_fig = filtered_df.plot(x='FY', y='raw', color='series_id',
                                            title="Raw Index Value by Series",
                                            labels={'FY': 'Fiscal Year', 'raw': 'Raw Index', 'series_id': 'Series ID'})
            if hasattr(raw_plot_fig, 'layout'):
                raw_plot_fig.layout.autosize = True
            plots_to_render.append(
                Card(Plotly(raw_plot_fig, config={'responsive': True}), title="Raw Index Plot"))
        except Exception as e:
            logger.error(
                f"Error generating raw index plot: {e}", exc_info=True)
            plots_to_render.append(Card(
                f"Could not generate raw index plot: {e}", title="Raw Index Plot Error"))
    else:
        logger.warning(
            "Skipping raw index plot: 'raw' or 'series_id' column missing.")

    if not plots_to_render:
        return Row(Markdown("### Required columns for plotting are missing."))

    return Row(*plots_to_render)


# --- Save/Load Configuration Widgets ---
index_name_input = TextInput(name="Index Configuration Name", placeholder="Enter a name for this configuration...")
save_config_button = Button(name="Save Current Configuration", button_type="primary")
file_download_config = FileDownload(
    label="Download Configuration", 
    embed=False, 
    button_type="success", 
    filename="index_config.json",
    visible=False # Initially hidden
)
load_config_input = FileInput(accept='.json', multiple=False) # multiple=False to ensure single file

# --- Save/Load Callbacks (Skeletons for now) ---
def handle_save_config(event: Any) -> None:
    """Gathers current UI settings, formats them as JSON, and prepares for download."""
    template.loading = True
    file_download_config.visible = False # Hide previous download link
    try:
        name = index_name_input.value
        if not name:
            name = f"Unnamed_Index_Config_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            index_name_input.value = name # Update UI with generated name
            pn.state.notifications.warning("Configuration name was empty, a default name has been generated.", duration=3000)

        # Gather forecast options
        forecast_opts_data: Dict[str, Any] = {'method': forecast_method_select.value}
        selected_method = forecast_method_select.value
        if selected_method == 'arima':
            if not (arima_p_input.value >= 0 and arima_d_input.value >= 0 and arima_q_input.value >= 0):
                pn.state.notifications.error("ARIMA P, D, and Q orders must be non-negative to save.", duration=4000)
                return
            forecast_opts_data['order'] = (arima_p_input.value, arima_d_input.value, arima_q_input.value)
        elif selected_method == 'exponential_smoothing':
            if es_seasonal_periods_input.value < 0:
                pn.state.notifications.error("Exponential Smoothing seasonal periods must be non-negative to save.", duration=4000)
                return
            forecast_opts_data['trend'] = es_trend_select.value
            forecast_opts_data['seasonal'] = es_seasonal_select.value
            forecast_opts_data['seasonal_periods'] = es_seasonal_periods_input.value if es_seasonal_periods_input.value > 0 else None
        elif selected_method == 'chained':
            if not chained_series_id_input.value:
                pn.state.notifications.error("Chained Series ID must be provided for 'chained' forecast to save.", duration=4000)
                return
            forecast_opts_data['series_id'] = chained_series_id_input.value
            forecast_opts_data['chain_type'] = 'rate' 

        config_data = {
            "indexName": name,
            "description": "User saved index configuration", # Placeholder description
            "version": "1.0",
            "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "seriesComponents": [{'id': sid} for sid in series_ids_widget.value],
            "outlays": outlays_widget.value.to_dict(orient='records') if isinstance(outlays_widget.value, pd.DataFrame) else [],
            "baseYear": base_year_widget.value,
            "forecastOptions": forecast_opts_data,
            # Save other relevant UI states if needed, e.g., fys_slider.value
            "fiscalYearRangeForPlot": fys_slider.value 
        }

        json_string = json.dumps(config_data, indent=2)
        bytes_io = BytesIO(json_string.encode())
        
        file_download_config.file = bytes_io
        file_download_config.filename = f"{name.replace(' ', '_').lower()}_config.json"
        file_download_config.visible = True # Make download button visible
        pn.state.notifications.success(f"Configuration '{name}' ready. Click the download button.", duration=4000)

    except Exception as e:
        logger.error(f"Error preparing configuration for saving: {e}", exc_info=True)
        pn.state.notifications.error(f"Error saving configuration: {e}", duration=5000)
    finally:
        template.loading = False

save_config_button.on_click(handle_save_config)

def handle_load_config(event: param.parameterized.Event) -> None: 
    """Loads a JSON configuration file and applies its settings to the UI."""
    if not load_config_input.value:
        return # No file selected or event triggered without a file

    template.loading = True
    try:
        json_string = load_config_input.value.decode('utf-8')
        loaded_config = json.loads(json_string)

        # Apply configuration to UI elements
        index_name_input.value = loaded_config.get("indexName", "Loaded Config")
        
        # Series IDs - this might trigger update_main_indices if it's watched correctly
        # For robustness, ensure series_ids_widget.options includes these before setting value
        loaded_series_ids = [s['id'] for s in loaded_config.get("seriesComponents", [])]
        current_options = list(series_ids_widget.options)
        for sid in loaded_series_ids:
            if sid not in current_options:
                current_options.append(sid) # Add if missing, ideally fetch title too
        series_ids_widget.options = current_options
        series_ids_widget.value = loaded_series_ids
        
        base_year_widget.value = loaded_config.get("baseYear", 2023)
        
        # Outlays
        outlays_data = loaded_config.get("outlays", [])
        if outlays_data: # Check if outlays_data is not empty
            # Ensure columns match what Tabulator expects (['sum'] + str_numbers)
            # The current outlays_widget is initialized with columns=['sum'] + [str(i) for i in range(10)]
            # If loaded data has different keys, it needs transformation or Tabulator needs re-init.
            # Assuming loaded_config['outlays'] is a list of dicts that can form a DataFrame.
            try:
                outlays_df = pd.DataFrame.from_records(outlays_data)
                 # Ensure all expected columns are present, fill with 0 if missing from loaded config
                for col in outlay_columns: # outlay_columns defined globally
                    if col not in outlays_df.columns:
                        outlays_df[col] = 0.0 
                outlays_widget.value = outlays_df[outlay_columns] # Reorder and select to match expected
            except Exception as e_outlays:
                logger.error(f"Error processing loaded outlays data: {e_outlays}", exc_info=True)
                pn.state.notifications.warning("Could not fully parse outlays from config. Check structure.", duration=4000)
        else: # If no outlays in config, reset to default or keep current? For now, reset.
            outlays_widget.value = pd.DataFrame(columns=outlay_columns, data=initial_outlay_data)


        # Forecast Options
        forecast_opts = loaded_config.get("forecastOptions", {})
        forecast_method_select.value = forecast_opts.get("method", "median")
        # update_forecast_options_ui will be triggered by the above change

        # Now, update specific forecast widgets based on the method
        # This needs to happen *after* update_forecast_options_ui has run,
        # or ensure the widgets are visible before setting their values.
        # A slight delay or pn.state.execute_later might be needed if direct assignment fails due to visibility.
        # For now, direct assignment:
        if forecast_opts.get("method") == 'arima':
            arima_order = forecast_opts.get("order", (1, 1, 0))
            if len(arima_order) == 3:
                arima_p_input.value, arima_d_input.value, arima_q_input.value = arima_order
        elif forecast_opts.get("method") == 'exponential_smoothing':
            es_trend_select.value = forecast_opts.get("trend", "add")
            es_seasonal_select.value = forecast_opts.get("seasonal") # Handles None correctly
            es_seasonal_periods_input.value = forecast_opts.get("seasonal_periods", 0) # Default to 0 if None/missing
        elif forecast_opts.get("method") == 'chained':
            chained_series_id_input.value = forecast_opts.get("series_id", "")
            
        # Other UI elements
        if "fiscalYearRangeForPlot" in loaded_config:
            fys_slider.value = tuple(loaded_config["fiscalYearRangeForPlot"])

        pn.state.notifications.success(f"Configuration '{loaded_config.get('indexName', 'N/A')}' loaded and applied.", duration=3000)
        
        # Explicitly trigger update_main_indices if not automatically done by all widget changes
        # This is important if some widget changes didn't trigger it due to complex dependency watching
        # or if we want a single consolidated update after all settings are applied.
        # For now, relying on @pn.depends with watch=True. If issues, a manual trigger button or
        # pn.state.execute_soon(update_main_indices_button.param.trigger('clicks')) could be used
        # if update_main_indices was attached to a (hidden) button.

    except Exception as e:
        logger.error(f"Failed to load or parse configuration file: {e}", exc_info=True)
        pn.state.notifications.error(f"Error loading configuration: {e}", duration=5000)
    finally:
        load_config_input.value = None # Reset FileInput to allow loading same file again
        template.loading = False

load_config_input.param.watch(handle_load_config, 'value')


# --- Save/Load Configuration Widgets ---
index_name_input = TextInput(name="Index Configuration Name", placeholder="Enter a name for this configuration...")
save_config_button = Button(name="Save Current Configuration", button_type="primary")
file_download_config = FileDownload(
    label="Download Configuration", 
    embed=False, 
    button_type="success", 
    filename="index_config.json",
    visible=False 
)
load_config_input = FileInput(accept='.json', multiple=False) 

# --- Export to Excel Widgets ---
export_excel_button = Button(name="Export Current Index to Excel", button_type="primary")
file_download_excel = FileDownload(
    label="Download Excel Report",
    embed=False,
    button_type="success",
    filename="index_report.xlsx", # Default filename
    visible=False
)

# --- Testable Helper Functions for Config/Export ---

def _build_config_data_dict(
    index_name_val: str,
    series_ids_val: List[str],
    outlays_df_val: pd.DataFrame, # Changed from outlays_val to outlays_df_val for clarity
    base_year_val: int,
    forecast_method_val: str,
    arima_p_val: int, arima_d_val: int, arima_q_val: int,
    es_trend_val: Optional[str], es_seasonal_val: Optional[str], 
    es_seasonal_periods_val: Optional[int],
    chained_series_id_val: str,
    fys_slider_val: Tuple[int, int]
) -> Dict[str, Any]:
    """Builds the configuration data dictionary from provided values."""
    
    forecast_opts_data: Dict[str, Any] = {'method': forecast_method_val}
    if forecast_method_val == 'arima':
        forecast_opts_data['order'] = (arima_p_val, arima_d_val, arima_q_val)
    elif forecast_method_val == 'exponential_smoothing':
        forecast_opts_data['trend'] = es_trend_val
        forecast_opts_data['seasonal'] = es_seasonal_val
        forecast_opts_data['seasonal_periods'] = es_seasonal_periods_val if es_seasonal_periods_val is not None and es_seasonal_periods_val > 0 else None
    elif forecast_method_val == 'chained':
        forecast_opts_data['series_id'] = chained_series_id_val
        forecast_opts_data['chain_type'] = 'rate'

    config_data = {
        "indexName": index_name_val,
        "description": "User saved index configuration",
        "version": "1.0",
        "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "seriesComponents": [{'id': sid} for sid in series_ids_val],
        "outlays": outlays_df_val.to_dict(orient='records') if isinstance(outlays_df_val, pd.DataFrame) else [],
        "baseYear": base_year_val,
        "forecastOptions": forecast_opts_data,
        "fiscalYearRangeForPlot": fys_slider_val
    }
    return config_data

def _build_excel_metadata_dict(
    export_timestamp_val: str,
    index_name_val: str,
    base_year_val: int,
    fys_slider_val: Tuple[int, int],
    active_series_ids_val: List[str],
    main_data_df_for_meta: pd.DataFrame, # Used to extract titles
    outlays_df_val: pd.DataFrame, # Changed from outlays_val to outlays_df_val
    forecast_method_val: str,
    arima_p_val: int, arima_d_val: int, arima_q_val: int,
    es_trend_val: Optional[str], es_seasonal_val: Optional[str],
    es_seasonal_periods_val: Optional[int],
    chained_series_id_val: str
) -> Dict[str, Any]:
    """Builds the metadata dictionary for Excel export from provided values."""
    
    component_series_list = []
    if not main_data_df_for_meta.empty and \
       'series_id' in main_data_df_for_meta.columns and \
       'title' in main_data_df_for_meta.columns:
        unique_series_info = main_data_df_for_meta[
            main_data_df_for_meta['series_id'].isin(active_series_ids_val)
        ][['series_id', 'title']].drop_duplicates()
        component_series_list = unique_series_info.apply(
            lambda x: f"{x['title']} ({x['series_id']})", axis=1
        ).tolist()
    elif active_series_ids_val:
        component_series_list = active_series_ids_val

    metadata_dict = {
        "Report Generated At": export_timestamp_val,
        "Index Configuration Name": index_name_val if index_name_val else "N/A (Not Saved/Named)",
        "Base Year": base_year_val,
        "Displayed Fiscal Year Range": fys_slider_val,
        "Component Series": ", ".join(component_series_list) if component_series_list else "N/A",
        "Outlay Weights": outlays_df_val.to_dict(orient='records') if isinstance(outlays_df_val, pd.DataFrame) else "N/A",
        "Forecast Method": forecast_method_val,
    }

    if forecast_method_val == 'arima':
        metadata_dict["ARIMA Order (p,d,q)"] = (arima_p_val, arima_d_val, arima_q_val)
    elif forecast_method_val == 'exponential_smoothing':
        metadata_dict["Exponential Smoothing Trend"] = es_trend_val
        metadata_dict["Exponential Smoothing Seasonal"] = es_seasonal_val
        metadata_dict["Exponential Smoothing Seasonal Periods"] = es_seasonal_periods_val
    elif forecast_method_val == 'chained':
        metadata_dict["Chained Series ID"] = chained_series_id_val
        
    return metadata_dict


# --- Save/Load Callbacks ---
def handle_save_config(event: Any) -> None:
    """Gathers current UI settings, formats them as JSON, and prepares for download."""
    template.loading = True
    file_download_config.visible = False 
    try:
        name_val = index_name_input.value
        if not name_val:
            name_val = f"Unnamed_Index_Config_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            index_name_input.value = name_val 
            pn.state.notifications.warning("Configuration name was empty, a default name has been generated.", duration=3000)

        # Validation for forecast parameters before calling helper
        if forecast_method_select.value == 'arima':
            if not (arima_p_input.value >= 0 and arima_d_input.value >= 0 and arima_q_input.value >= 0):
                pn.state.notifications.error("ARIMA P, D, and Q orders must be non-negative to save.", duration=4000)
                template.loading = False; return
        elif forecast_method_select.value == 'exponential_smoothing':
            if es_seasonal_periods_input.value < 0:
                pn.state.notifications.error("Exponential Smoothing seasonal periods must be non-negative to save.", duration=4000)
                template.loading = False; return
        elif forecast_method_select.value == 'chained':
            if not chained_series_id_input.value:
                pn.state.notifications.error("Chained Series ID must be provided for 'chained' forecast to save.", duration=4000)
                template.loading = False; return

        config_data = _build_config_data_dict(
            index_name_val=name_val,
            series_ids_val=series_ids_widget.value,
            outlays_df_val=outlays_widget.value, # Pass DataFrame directly
            base_year_val=base_year_widget.value,
            forecast_method_val=forecast_method_select.value,
            arima_p_val=arima_p_input.value,
            arima_d_val=arima_d_input.value,
            arima_q_val=arima_q_input.value,
            es_trend_val=es_trend_select.value,
            es_seasonal_val=es_seasonal_select.value,
            es_seasonal_periods_val=es_seasonal_periods_input.value,
            chained_series_id_val=chained_series_id_input.value,
            fys_slider_val=fys_slider.value
        )
        
        json_string = json.dumps(config_data, indent=2)
        bytes_io = BytesIO(json_string.encode())
        
        file_download_config.file = bytes_io
        file_download_config.filename = f"{name_val.replace(' ', '_').lower()}_config.json"
        file_download_config.visible = True 
        pn.state.notifications.success(f"Configuration '{name_val}' ready. Click the download button.", duration=4000)

    except Exception as e:
        logger.error(f"Error preparing configuration for saving: {e}", exc_info=True)
        pn.state.notifications.error(f"Error saving configuration: {e}", duration=5000)
    finally:
        template.loading = False

save_config_button.on_click(handle_save_config)

def handle_load_config(event: param.parameterized.Event) -> None: 
    """Loads a JSON configuration file and applies its settings to the UI."""
    if not load_config_input.value:
        return 

    template.loading = True
    try:
        json_string = load_config_input.value.decode('utf-8')
        loaded_config = json.loads(json_string)

        index_name_input.value = loaded_config.get("indexName", "Loaded Config")
        
        loaded_series_ids = [s['id'] for s in loaded_config.get("seriesComponents", [])]
        current_options = list(series_ids_widget.options)
        for sid in loaded_series_ids:
            if sid not in current_options:
                current_options.append(sid) 
        series_ids_widget.options = current_options
        series_ids_widget.value = loaded_series_ids
        
        base_year_widget.value = loaded_config.get("baseYear", 2023)
        
        outlays_data = loaded_config.get("outlays", [])
        if outlays_data: 
            try:
                outlays_df = pd.DataFrame.from_records(outlays_data)
                for col in outlay_columns: 
                    if col not in outlays_df.columns:
                        outlays_df[col] = 0.0 
                outlays_widget.value = outlays_df[outlay_columns] 
            except Exception as e_outlays:
                logger.error(f"Error processing loaded outlays data: {e_outlays}", exc_info=True)
                pn.state.notifications.warning("Could not fully parse outlays from config. Check structure.", duration=4000)
        else: 
            outlays_widget.value = pd.DataFrame(columns=outlay_columns, data=initial_outlay_data)

        forecast_opts = loaded_config.get("forecastOptions", {})
        forecast_method_select.value = forecast_opts.get("method", "median")
        
        if forecast_opts.get("method") == 'arima':
            arima_order = forecast_opts.get("order", (1, 1, 0))
            if len(arima_order) == 3:
                arima_p_input.value, arima_d_input.value, arima_q_input.value = arima_order
        elif forecast_opts.get("method") == 'exponential_smoothing':
            es_trend_select.value = forecast_opts.get("trend", "add")
            es_seasonal_select.value = forecast_opts.get("seasonal") 
            es_seasonal_periods_input.value = forecast_opts.get("seasonal_periods", 0) 
        elif forecast_opts.get("method") == 'chained':
            chained_series_id_input.value = forecast_opts.get("series_id", "")
            
        if "fiscalYearRangeForPlot" in loaded_config:
            fys_slider.value = tuple(loaded_config["fiscalYearRangeForPlot"])

        pn.state.notifications.success(f"Configuration '{loaded_config.get('indexName', 'N/A')}' loaded and applied.", duration=3000)
        
    except Exception as e:
        logger.error(f"Failed to load or parse configuration file: {e}", exc_info=True)
        pn.state.notifications.error(f"Error loading configuration: {e}", duration=5000)
    finally:
        load_config_input.value = None 
        template.loading = False

load_config_input.param.watch(handle_load_config, 'value')

# --- Export to Excel Callback ---
def handle_export_excel(event: Any) -> None:
    """Gathers current index data and metadata, then exports to an Excel file."""
    template.loading = True
    file_download_excel.visible = False 
    try:
        main_data_df = df_widget.value.copy() 
        if main_data_df.empty:
            pn.state.notifications.warning("No data in the main table to export.", duration=3000)
            template.loading = False; return # Added return after setting loading to false

        export_timestamp_val = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        metadata_dict = _build_excel_metadata_dict(
            export_timestamp_val=export_timestamp_val,
            index_name_val=index_name_input.value,
            base_year_val=base_year_widget.value,
            fys_slider_val=fys_slider.value,
            active_series_ids_val=series_ids_widget.value,
            main_data_df_for_meta=main_data_df, # Pass the actual data for title extraction
            outlays_df_val=outlays_widget.value, # Pass DataFrame directly
            forecast_method_val=forecast_method_select.value,
            arima_p_val=arima_p_input.value,
            arima_d_val=arima_d_input.value,
            arima_q_val=arima_q_input.value,
            es_trend_val=es_trend_select.value,
            es_seasonal_val=es_seasonal_select.value,
            es_seasonal_periods_val=es_seasonal_periods_input.value,
            chained_series_id_val=chained_series_id_input.value
        )
        
        metadata_df = pd.DataFrame(list(metadata_dict.items()), columns=['Parameter', 'Value'])

        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            main_data_df.to_excel(writer, sheet_name='Index_Data', index=False)
            metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
        excel_buffer.seek(0) # Rewind the buffer

        # Prepare for Download
        file_download_excel.file = excel_buffer
        export_filename = "index_report.xlsx"
        if index_name_input.value:
            export_filename = f"{index_name_input.value.replace(' ', '_').lower()}_report.xlsx"
        file_download_excel.filename = export_filename
        file_download_excel.visible = True
        pn.state.notifications.success("Excel report generated. Click the download button.", duration=4000)

    except Exception as e:
        logger.error(f"Failed to generate Excel report: {e}", exc_info=True)
        pn.state.notifications.error(f"Error generating Excel report: {e}", duration=5000)
    finally:
        template.loading = False

export_excel_button.on_click(handle_export_excel)


# --- Page Layout and Servable ---
sidebar_search_card = Card(
    search_input,
    search_limit_input,
    search_button,
    title="Search & Add Series Data"
)

sidebar_index_config_card = Card(
    series_ids_widget, 
    base_year_widget,
    title="Core Index Parameters"
)

sidebar_forecast_card = Card(
    forecast_method_select,
    forecast_options_ui, 
    title="Forecast Configuration"
)

sidebar_manage_config_card = Card(
    index_name_input,
    save_config_button,
    file_download_config, 
    pn.Spacer(height=10),
    Markdown("---"), 
    load_config_input,  
    title="Manage Index Configurations"
)

sidebar_export_card = Card( # New card for export
    export_excel_button,
    file_download_excel,
    title="Export Index Data"
)


template = FastListTemplate(
    title='FRED Economic Index Analysis Dashboard',
    sidebar=[
        sidebar_search_card,
        sidebar_index_config_card,
        sidebar_forecast_card,
        sidebar_manage_config_card,
        sidebar_export_card # Added new export card
    ],
    main=[ 
        pn.Row(outlays_widget, fys_slider), 
        Tabs(
            ("Index Table", df_widget),
            ("Index Perspective", indices)
        ),
        graph_dynamic_indices # Graph display
    ],
    accent_base_color="#2c3e50", 
    header_background="#34495e", 
    theme=pn.theme.MaterialDarkTheme, 
    sidebar_width=380, # Slightly wider sidebar for new options
)

# Make the application servable
template.servable()
