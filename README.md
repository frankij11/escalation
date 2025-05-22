# Escalation Analysis Tool

## Overview and Purpose

This tool allows users to analyze and visualize economic escalation indices using data from the Federal Reserve Economic Data (FRED) service. It provides an interactive dashboard built with Panel to explore economic trends and create custom indices.

Key features include:
*   Search for economic series directly from the FRED database.
*   Create custom-weighted economic indices based on selected series, outlay profiles, and base years.
*   Visualize raw data, calculated rates, and generated indices.
*   Interactive dashboard for easy data exploration and manipulation.
*   Tabular and pivot-table views of data using Panel's Tabulator and Perspective components.

## Setup and Installation Instructions

### Prerequisites

*   Python 3.7 or higher.

### Steps

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/yourusername/escalation-tool.git
    ```
    (Replace the URL with the actual repository URL if available.)

2.  **Navigate to the project directory:**
    ```bash
    cd escalation-tool
    ```

3.  **Set up FRED API Key:**
    A FRED API key is required to fetch data from the FRED service.
    *   Obtain a free API key from the [FRED website](https://fred.stlouisfed.org/docs/api/api_key.html).
    *   Set the API key as an environment variable:
        *   For Linux/macOS:
            ```bash
            export FRED_API_KEY='your_api_key_here'
            ```
        *   For Windows (Command Prompt):
            ```bash
            set FRED_API_KEY=your_api_key_here
            ```
        *   For Windows (PowerShell):
            ```powershell
            $env:FRED_API_KEY='your_api_key_here'
            ```
        Replace `your_api_key_here` with the actual API key you obtained.

4.  **Install dependencies:**
    Ensure you have Python and pip installed. Then, install the required packages using the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```

## Example Usage / Running the Application

To run the Panel application:

```bash
python app.py
```

This will start a local web server. The application logs will indicate the URL to access the dashboard. By default, `app.py` is configured to serve the application on port 443 (e.g., `http://localhost:443` or `https://localhost:443` if SSL is configured, though this script does not configure SSL by default).

**Note:**
*   Using port 443 typically requires administrator/sudo privileges on Linux and macOS systems.
*   This port is often chosen for deployment environments (like Hugging Face Spaces or Render) that expect applications to run on standard HTTPS ports. For local development, you might consider changing the port in `app.py` to a non-privileged port (e.g., 8080 or 7860) if you encounter issues.

Once started, open the provided URL in your web browser to interact with the Escalation Analysis Tool.

## Dependencies

All project dependencies are listed in the `requirements.txt` file. The main dependencies include:

*   **pandas:** For data manipulation and analysis.
*   **panel:** For creating the interactive web dashboard.
*   **requests:** For making HTTP requests to the FRED API.
*   **plotly:** Used as the plotting backend for pandas and Panel to generate interactive visualizations.
*   **hvplot:** While not directly called in all parts, it can be used with Panel and pandas for quick interactive plots (and `pd.options.plotting.backend='plotly'` can leverage parts of its ecosystem).

## Contribution Guidelines

Contributions are welcome! If you have suggestions for improvements, find bugs, or want to add new features, please feel free to:
*   Open an issue to discuss the changes.
*   Submit a pull request with your contributions.

**Pull Request Process:**

1.  Fork the repository.
2.  Create your feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request against the `main` (or `master`) branch of this repository.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
