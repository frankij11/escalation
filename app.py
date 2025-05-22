"""
Serves Panel applications, configured for Hugging Face Spaces and other environments.

This script constructs and executes the `panel serve` command with appropriate
configurations for port, address, and WebSocket origins. It's designed to be
used in deployment scenarios like Hugging Face Spaces or Render.
"""
import os
import logging
from subprocess import Popen
from typing import List

# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("Starting Panel Application Server...")

# --- Configuration ---

# Space separated list of .py or .ipynb files to serve
APPS_TO_SERVE: str = "escalation.py"
# INDEX_PAGE was removed as it's not currently used.

# Port and Address Configuration
# The port is hardcoded to 443 for typical HTTPS deployments.
# Consider using os.environ.get("PORT", "7860") for more flexibility if needed.
PORT: str = "443"
ADDRESS: str = "0.0.0.0"  # Listen on all available network interfaces

# Construct the command for `panel serve`
command: List[str] = [
    "panel",
    "serve",
    *APPS_TO_SERVE.split(" "),  # Spreads the app names into the command list
    "--port",
    PORT,
    "--address",
    ADDRESS,
    # Explicitly allowed WebSocket origins for security.
    # Removed "*", as specific origins are preferred.
    "--allow-websocket-origin",
    "localhost",  # For local development and testing
    "--allow-websocket-origin",
    # For Render deployment (ensure port matches if changed)
    "*escalation.onrender.com:443",
    "--allow-websocket-origin",
    "*.huggingface.co",  # For Hugging Face Spaces deployment
    # Example for adding log-level, if needed:
    # "--log-level",
    # "debug"
]

# Add process and thread count for non-Windows (e.g., Linux) environments for better performance.
if os.name != "nt":
    command.extend(["--num-procs", "4", "--num-threads", "4"])

logging.info(f"Executing command: {' '.join(command)}")

# Start the Panel server
try:
    # pylint: disable=consider-using-with # Popen is not a context manager in this usage
    worker = Popen(command)
    worker.wait()  # Wait for the server process to complete
    logging.info("Panel Application Server finished.")
except FileNotFoundError:
    logging.error(
        "Error: 'panel' command not found. Ensure Panel is installed and in PATH."
    )
except Exception as e:
    logging.error(
        f"An error occurred while running the Panel server: {e}", exc_info=True)
