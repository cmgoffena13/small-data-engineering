import os
from pathlib import Path

from dotenv import load_dotenv

path = Path(".").resolve()

load_dotenv(path)

DATA_WAREHOUSE_URL = os.environ["DATA_WAREHOUSE_URL"]
