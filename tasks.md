# AI-Powered Task Tracker for Escalation Analysis Tool

## Recommended Features and Improvements

### Task: Implement Comprehensive Unit Tests for `fred.py`
- [x] **Description:** Enhance `fred.py` by adding comprehensive unit tests using the `pytest` framework and `unittest.mock` to cover various scenarios for data fetching, transformation methods like `make_index` and `get_series_from_search`, and edge cases, including the new forecasting methods. *(Completed: Basic tests for new forecasting methods added. Could be further expanded for more edge cases.)*
- **Prompt for AI:** "Review `fred.py` and the existing tests in `tests/test_fred.py`. Expand the test suite to cover all public methods, focusing on mocking API calls effectively. Ensure tests for `make_index` verify the correctness of calculations with various inputs (different base years, outlay profiles, forecast options). Add tests for error handling and different API response scenarios (e.g., empty results, API errors for all forecast types)."
- **Tags:** #testing #refactor #fred_py
- **Priority:** High
- **Completed:** [x] *(2024-07-30)*

### Task: Refactor API Key Handling
- [ ] **Description:** Improve the management of the FRED API key. Instead of relying solely on an environment variable (`FRED_API_KEY`), implement a more robust strategy, such as using a dedicated configuration file (e.g., `.env` file loaded with `python-dotenv`, or a simple `config.ini`).
- **Prompt for AI:** "Refactor the application (primarily `fred.py`) to manage the FRED_API_KEY. Implement support for reading the key from an `.env` file using `python-dotenv`. Ensure the application falls back to checking the environment variable if the `.env` file or key is not found. Update `README.md` with instructions for using the `.env` file. Add `python-dotenv` to `requirements.txt` and `pyproject.toml`."
- **Tags:** #config #security #best_practice
- **Priority:** Medium
- **Completed:** [ ]

### Task: Develop a Command-Line Interface for `fred.py`
- [ ] **Description:** Create a command-line interface (CLI) for `fred.py` to allow users to fetch FRED data, perform searches, and generate indices directly from the terminal. This would make the core logic of `fred.py` usable independently of the Panel application.
- **Prompt for AI:** "Using `argparse` or `click`, add a CLI to `fred.py`. Implement subcommands such as `search <term>`, `get-series <series_id>`, and `make-index <series_id_or_search_term> --base-year YYYY --outlays X,Y,Z --forecast-method <method> --forecast-options <json_string>`. The output should be human-readable, possibly CSV or JSON for data."
- **Tags:** #feature #cli #fred_py
- **Priority:** Medium
- **Completed:** [ ]

### Task: Enhance Panel Application UI/UX
- [x] **Description:** Review and enhance the user interface (UI) and user experience (UX) of the Panel application in `escalation.py`. This could involve improving layout, adding more interactive controls, providing better visual feedback, or optimizing performance for large datasets. *(Partially completed by adding forecast controls, loading indicators, input validation, save/load config, and export to Excel. Further general UX review can be a follow-up).*
- **Prompt for AI:** "Review `escalation.py`. Identify further areas for UI/UX improvement beyond the forecast controls. Suggestions:
    1.  Ensure persistent user notifications are used consistently for all operations and errors.
    2.  Refine the layout of controls and plots for better readability and usability on various screen sizes (responsive design).
    3.  Consider options for customizing plot appearances (e.g., colors, line styles) via the UI.
    4.  Investigate and implement performance optimizations if UI becomes slow with many series or long date ranges."
- **Tags:** #feature #ui_ux #panel #escalation_py
- **Priority:** Medium
- **Completed:** [x] *(2024-07-30 - ongoing improvements)*

### Task: Implement Advanced Forecasting Methods in `fred.py`
- [x] **Description:** Extend `fred.py` to include more sophisticated time series forecasting methods beyond the current simple median-based forecast. This would provide users with more powerful analytical capabilities. *(Completed: Median, Mean, ARIMA, Exponential Smoothing, and a simplified Chained method are implemented in `fred.py` and integrated into `escalation.py` UI).*
- **Prompt for AI:** "This task is complete."
- **Tags:** #feature #analysis #forecasting #fred_py
- **Priority:** High
- **Completed:** [x] *(2024-07-30)*

### Task: Implement Save/Load Index Configuration
- [x] **Description:** Allow users to save their current index configuration (series, weights, base year, forecast settings) to a JSON file and load it back into the application.
- **Prompt for AI:** "Implement UI and logic in `escalation.py` for saving the current index parameters to a JSON file and for loading parameters from such a file to restore the application state. This includes UI elements for naming, saving, and uploading configurations."
- **Tags:** #feature #ui_ux #index_management #escalation_py
- **Priority:** High
- **Completed:** [x] *(2024-07-30)*

### Task: Implement Export to Excel
- [x] **Description:** Allow users to export the currently displayed index data, along with its generating parameters (metadata), to an Excel (XLSX) file.
- **Prompt for AI:** "Add functionality to `escalation.py` to export the main index data table and a sheet of metadata (parameters used) to an Excel file. This will involve using Pandas ExcelWriter and providing a download mechanism."
- **Tags:** #feature #export #excel #escalation_py
- **Priority:** High
- **Completed:** [x] *(2024-07-30)*

### Task: Containerize Application with Docker
- [ ] **Description:** Create a `Dockerfile` to containerize the application, making it easier to deploy and run in various environments.
- **Prompt for AI:** "Create a `Dockerfile` for the Python application. It should:
    1. Use an official Python base image (e.g., `python:3.9-slim`).
    2. Set up a working directory.
    3. Copy `requirements.txt` and `pyproject.toml` and install dependencies (preferably using Poetry if `pyproject.toml` is primary).
    4. Copy the application files (`app.py`, `fred.py`, `escalation.py`, and any other necessary assets).
    5. Expose the port used by `app.py` (default 443, but consider making this configurable or using a more standard non-privileged port like 8000 for Docker).
    6. Specify the command to run the application (`python app.py`).
    7. Include instructions in `README.md` on how to build and run the Docker container, including passing the `FRED_API_KEY` environment variable."
- **Tags:** #deployment #docker #best_practice
- **Priority:** Medium
- **Completed:** [ ]

### Task: Refine Chained Forecasting Logic
- [ ] **Description:** The current 'chained' forecast uses the median rate of the chained series. Explore and implement more sophisticated chaining logic, such as aligning growth patterns or value offsets.
- **Prompt for AI:** "Review the 'chained' forecasting method in `fred.py`. Enhance it to allow options like applying the percentage change of the chained series to the last actual value of the main series, or maintaining a value offset. Update UI in `escalation.py` if new configuration options are needed."
- **Tags:** #feature #analysis #forecasting #fred_py
- **Priority:** Low
- **Completed:** [ ]

### Task: Comprehensive UI Testing for Panel App
- [ ] **Description:** Implement UI testing for the Panel application in `escalation.py` using a suitable framework (e.g., Selenium, Playwright with pytest) to automate testing of user interactions and visual outputs.
- **Prompt for AI:** "Research and set up a UI testing framework for the Panel application. Create test scripts to cover key user workflows: searching for series, creating a custom index, selecting different forecast methods, and verifying that the plots and tables update correctly."
- **Tags:** #testing #ui_ux #panel #escalation_py
- **Priority:** Medium
- **Completed:** [ ]

### Task: Advanced Index Management UI
- [ ] **Description:** Enhance the index management UI to support a list of saved configurations (if using browser local storage or a backend), allowing users to directly load, rename, or delete them from the UI without manual file handling for each operation.
- **Prompt for AI:** "Investigate options for storing multiple index configurations (e.g., browser local storage). If feasible, implement a UI in `escalation.py` to list saved configurations, allow users to select one to load, and provide options to rename or delete saved configurations."
- **Tags:** #feature #ui_ux #index_management
- **Priority:** Medium
- **Completed:** [ ]

### Task: Customizable Excel Export Formatting
- [ ] **Description:** Allow users some control over the Excel export, such as choosing which columns to include, number formatting, or basic styling.
- **Prompt for AI:** "Extend the 'Export to Excel' functionality in `escalation.py`. Add UI options (e.g., checkboxes for columns, number format selection) to customize the content and appearance of the exported Excel file. Update the `handle_export_excel` function to apply these customizations."
- **Tags:** #feature #export #excel
- **Priority:** Low
- **Completed:** [ ]
