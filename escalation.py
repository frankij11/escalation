"""
Panel application for analyzing and visualizing economic escalation indices from FRED data.

This application allows users to:
- Search for economic series from FRED.
- Generate custom economic indices based on selected series, outlays, and base years.
- View data in tabular and Perspective (pivot table) formats.
- Visualize indices using interactive plots.
"""
import logging
# Added basic typing imports, will add more specific ones like pd.DataFrame later
from typing import Any, List, Union, Tuple, Dict

import pandas as pd
import panel as pn
from panel.widgets import Tabulator, MultiChoice, Select, TextInput, IntInput, Button, IntRangeSlider
from panel.pane import Perspective, Plotly, Markdown
from panel.layout import Card, Row, Tabs, Column # Added Column
from panel.template import FastListTemplate # Added FastListTemplate
# hvplot.pandas and plotly.express are not directly used.
# The plotly backend for pandas plotting is enabled via pd.options.

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
try:
    initial_series_data = esc.make_index_from_search(
        ["Gross Domestic Product Deflator", "CPI", "PPI"],
        limit=1
    ).drop_duplicates()
except Exception as e:
    logger.error(f"Failed to load initial series data: {e}")
    initial_series_data = pd.DataFrame()  # Start with empty on error

# Rename columns for display in Tabulator and Perspective
processed_initial_series = _rename_numeric_cols_to_str(initial_series_data)

df_widget = pn.widgets.Tabulator(
    processed_initial_series, pagination='remote', page_size=10)
indices = pn.pane.Perspective(
    processed_initial_series, toggle_config=False, theme="material-dark")

# Widgets for user inputs
series_options: List[str] = []
if 'id' in processed_initial_series.columns:  # Check if 'id' column exists
    series_options = processed_initial_series.id.unique().tolist()
elif not processed_initial_series.empty:  # Log if 'id' is missing but df is not empty
    logger.warning(
        "'id' column not found in initial_series_data. MultiChoice for series_ids will be empty.")

series_ids_widget = pn.widgets.MultiChoice(
    name="Select Series for Index", options=series_options)  # Renamed for clarity
base_year_widget = pn.widgets.Select(
    name="Base Year", value=2023, options=list(range(1970, 2061)))  # Renamed

# Define initial outlay DataFrame structure
# Columns are 'sum' followed by numeric strings '0' through '9'
outlay_columns = ['sum'] + [str(i) for i in range(10)]
# Ensure data matches column structure
initial_outlay_data = [[1, 0.75, 0.25] + [0.0] * 8]
outlays_widget = pn.widgets.Tabulator(  # Renamed
    name="Outlay Profile (Yearly %)",
    value=pd.DataFrame(columns=outlay_columns, data=initial_outlay_data),
    layout='fit_data',  # Ensure table fits data
    disabled=True  # Initially disabled, enable if editable outlays are desired
)

fys_slider = pn.widgets.IntRangeSlider(
    name="Fiscal Year Range for Plot", start=1970, end=2060, value=(2000, 2025))  # Renamed

search_input = pn.widgets.TextInput(
    name="Search FRED Series (e.g., 'GDPDEF', 'CPIAUCSL')")  # Renamed
search_limit_input = pn.widgets.IntInput(
    name="Limit Search Results", start=1, value=1, end=5)  # Renamed
search_button = pn.widgets.Button(
    name="Search and Add Series to Table")  # Renamed


# --- Callback Functions ---

def add_series_data(event: Any) -> None:
    """
    Callback triggered by the search button (`search_button`).
    Fetches series data from FRED based on `search_input` and `search_limit_input`,
    then updates the Tabulator (`df_widget`) and Perspective (`indices`) panes.
    The search input field is cleared after the search.

    Args:
        event (Any): The event object from the button click (not used directly).
    """
    logger.info(
        f"Searching for series: {search_input.value} with limit: {search_limit_input.value}")
    if not search_input.value:
        logger.warning("Search input is empty. Skipping search.")
        pn.state.notifications.warning(
            "Search term cannot be empty.", duration=3000)
        return

    try:
        new_series_df = esc.make_index_from_search(
            search_input.value, limit=search_limit_input.value)
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


search_button.on_click(add_series_data)


@pn.depends(series_ids=series_ids_widget, outlays_df=outlays_widget, base_year=base_year_widget)
def update_main_indices(series_ids: List[str], outlays_df: pd.DataFrame, base_year: int) -> None:
    """
    Callback triggered by changes in `series_ids_widget`, `outlays_widget`, or `base_year_widget`.
    Recalculates the economic index using `esc.make_index` and updates the
    main Tabulator (`df_widget`) and Perspective (`indices`) panes.

    Args:
        series_ids (List[str]): Selected series IDs from `series_ids_widget`.
        outlays_df (pd.DataFrame): DataFrame from `outlays_widget.value`.
        base_year (int): Selected base year from `base_year_widget.value`.
    """
    logger.info(
        f"Updating indices for series: {series_ids}, base_year: {base_year}")
    logger.debug(f"Outlays DataFrame for update:\n{outlays_df}")

    if not series_ids:
        logger.warning("No series IDs selected. Cannot update index.")
        # Optionally clear the widgets or show a message
        # df_widget.object = pd.DataFrame()
        # indices.object = pd.DataFrame()
        return

    # Extract outlay values. Assuming the first row of outlays_df contains the relevant profile.
    # The structure from fred.py's make_index expects a tuple/list of floats.
    # The outlays_df has columns like '0', '1', ... which are string representations.
    try:
        # Ensure outlay_df has data and expected columns
        if outlays_df.empty:
            logger.error("Outlays DataFrame is empty. Cannot calculate index.")
            pn.state.notifications.error(
                "Outlay profile is missing.", duration=3000)
            return

        # Extract values from columns '0' through '9' (or as many as exist)
        outlay_values_str_cols = [str(i) for i in range(
            10) if str(i) in outlays_df.columns]
        if not outlay_values_str_cols:
            logger.error(
                "No valid outlay columns (e.g., '0', '1', ..) found in outlays_df.")
            pn.state.notifications.error(
                "Invalid outlay profile structure.", duration=3000)
            return

        # Take the first row for outlay percentages
        outlay_profile = tuple(
            outlays_df.iloc[0][outlay_values_str_cols].astype(float))
        logger.debug(f"Extracted outlay profile: {outlay_profile}")

        updated_df = esc.make_index(
            series_ids=series_ids, outlays=outlay_profile, base_year=base_year)

        processed_updated_df = _rename_numeric_cols_to_str(updated_df)

        df_widget.object = processed_updated_df
        # Perspective usually needs full object replacement
        indices.object = processed_updated_df
        logger.info("Successfully updated main index displays.")
    except Exception as e:
        logger.error(f"Error updating main indices: {e}", exc_info=True)
        pn.state.notifications.error(
            f"Error calculating index: {e}", duration=5000)


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


# --- Page Layout and Servable ---
# Using more specific types for Panel components in the template definition
sidebar_components: List[Card] = [ # More specific type for sidebar list
    Card(
        search_input,
        search_limit_input,
        search_button,
        title="Search FRED Series"
    )
]

main_layout = Column( # Explicitly define main_layout for clarity
    Row(outlays_widget, base_year_widget),
    series_ids_widget,
    fys_slider,
    Tabs(
        ("Index Table", df_widget),
        ("Index Perspective", indices)
    ),
    graph_dynamic_indices
)

template = FastListTemplate(
    title='FRED Economic Index Analysis Dashboard',
    sidebar=sidebar_components,
    main=[main_layout], # main takes a list of displayable objects
    accent_base_color="#2c3e50",
    header_background="#34495e",
    theme=pn.theme.MaterialDarkTheme,
    sidebar_width=350,
)

# Make the application servable
template.servable()
