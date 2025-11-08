"""Project-level utilities."""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

ENV = os.environ.get("ENV", "local")

