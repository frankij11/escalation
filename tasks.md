# AI-Powered Task Tracker for Escalation Analysis Tool

## Recommended Features and Improvements

### Task: Implement Comprehensive Unit Tests for `fred.py`
- [ ] **Description:** Enhance `fred.py` by adding comprehensive unit tests using the `pytest` framework and `requests-mock` (or `unittest.mock`) to cover various scenarios for data fetching, transformation methods like `make_index` and `get_series_from_search`, and edge cases.
- **Prompt for AI:** "Review `fred.py` and the existing tests in `tests/test_fred.py`. Expand the test suite to cover all public methods, focusing on mocking API calls effectively. Ensure tests for `make_index` verify the correctness of calculations with various inputs (different base years, outlay profiles). Add tests for error handling and different API response scenarios (e.g., empty results, API errors)."
- **Tags:** #testing #refactor #fred_py
- **Priority:** High
- **Completed:** [ ]

### Task: Refactor API Key Handling
- [ ] **Description:** Improve the management of the FRED API key. Instead of relying solely on an environment variable (`FRED_API_KEY`), implement a more robust strategy, such as using a dedicated configuration file (e.g., `.env` file loaded with `python-dotenv`, or a simple `config.ini`).
- **Prompt for AI:** "Refactor the application (primarily `fred.py`) to manage the FRED_API_KEY. Implement support for reading the key from an `.env` file using `python-dotenv`. Ensure the application falls back to checking the environment variable if the `.env` file or key is not found. Update `README.md` with instructions for using the `.env` file. Add `python-dotenv` to `requirements.txt` and `pyproject.toml`."
- **Tags:** #config #security #best_practice
- **Priority:** Medium
- **Completed:** [ ]

### Task: Develop a Command-Line Interface for `fred.py`
- [ ] **Description:** Create a command-line interface (CLI) for `fred.py` to allow users to fetch FRED data, perform searches, and generate indices directly from the terminal. This would make the core logic of `fred.py` usable independently of the Panel application.
- **Prompt for AI:** "Using `argparse` or `click`, add a CLI to `fred.py`. Implement subcommands such as `search <term>`, `get-series <series_id>`, and `make-index <series_id_or_search_term> --base-year YYYY --outlays X,Y,Z`. The output should be human-readable, possibly CSV or JSON for data."
- **Tags:** #feature #cli #fred_py
- **Priority:** Medium
- **Completed:** [ ]

### Task: Enhance Panel Application UI/UX
- [ ] **Description:** Review and enhance the user interface (UI) and user experience (UX) of the Panel application in `escalation.py`. This could involve improving layout, adding more interactive controls, providing better visual feedback, or optimizing performance for large datasets.
- **Prompt for AI:** "Review `escalation.py`. Identify areas for UI/UX improvement. Suggestions:
    1.  Implement persistent user notifications for long-running operations or errors (current `pn.state.notifications` are good, ensure they are used consistently).
    2.  Add a 'loading' indicator when data is being fetched or processed.
    3.  Improve the layout of controls and plots for better readability on various screen sizes.
    4.  Consider options for customizing plot appearances (e.g., colors, line styles) via the UI."
- **Tags:** #feature #ui_ux #panel #escalation_py
- **Priority:** Medium
- **Completed:** [ ]

### Task: Implement Advanced Forecasting Methods in `fred.py`
- [ ] **Description:** Extend `fred.py` to include more sophisticated time series forecasting methods beyond the current simple median-based forecast. This would provide users with more powerful analytical capabilities.
- **Prompt for AI:** "Research and implement one or two additional forecasting methods in `fred.py` (e.g., ARIMA, Exponential Smoothing using `statsmodels`). Modify the `make_index` method to allow selection of the forecasting method. Update the Panel application in `escalation.py` to include a UI control for choosing the forecast method."
- **Tags:** #feature #analysis #forecasting #fred_py
- **Priority:** High
- **Completed:** [ ]

### Task: Containerize Application with Docker
- [ ] **Description:** Create a `Dockerfile` to containerize the application, making it easier to deploy and run in various environments.
- **Prompt for AI:** "Create a `Dockerfile` for the Python application. It should:
    1. Use an official Python base image (e.g., `python:3.9-slim`).
    2. Set up a working directory.
    3. Copy `requirements.txt` and `pyproject.toml` (if used for install) and install dependencies.
    4. Copy the application files (`app.py`, `fred.py`, `escalation.py`, and any other necessary assets).
    5. Expose the port used by `app.py` (default 443, but consider making this configurable or using a more standard non-privileged port like 8000 for Docker).
    6. Specify the command to run the application (`python app.py`).
    7. Include instructions in `README.md` on how to build and run the Docker container, including passing the `FRED_API_KEY` environment variable."
- **Tags:** #deployment #docker #best_practice
- **Priority:** Medium
- **Completed:** [ ]
