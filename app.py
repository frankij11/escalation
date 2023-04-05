"""This files enables serving Panel apps on Hugging Face Spaces"""
import os
from subprocess import Popen

print("Starting Application", os.environ.get("fred_api_key"))

# CONFIGURE YOUR SETTINGS HERE

# Space separated list of .py or .ipynb files to serve
APPS_TO_SERVE = "escalation.py"
# Prefix of the index .py or .ipynb file. Must be in APPS_TO_SERVE too
#INDEX_PAGE = "index"

# NORMALLY NO NEED TO CHANGE THE BELOW
PORT ="10000"#os.environ.get("PORT", "7860")
ADDRESS = "0.0.0.0"
command = [
    "panel",
    "serve",
    *APPS_TO_SERVE.split(" "),
#    "--index",
#    INDEX_PAGE,
    "--port",
    PORT,
    "--address",
    ADDRESS,
    "--allow-websocket-origin",
    "localhost",

    "--allow-websocket-origin",
    "*onrender.com",

    "--allow-websocket-origin",
    "*escalation.onrender.com:443",
    "--allow-websocket-origin",
    "*.huggingface.co",
    # "--log-level",
    # "debug"
]
if os.name != "nt":
    command = command + ["--num-procs", "4", "--num-threads", "4"] 

print(" ".join(command))
worker = Popen(command) 
worker.wait()
