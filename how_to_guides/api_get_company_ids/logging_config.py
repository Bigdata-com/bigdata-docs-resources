import logging
import os
import sys


def setup_logging(log_dir="logs", log_file="company_ids.log"):
    os.makedirs(log_dir, exist_ok=True)
    # stderr is typically line-buffered on TTYs; avoids INFO appearing only after a full batch.
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.INFO)
    file_handler = logging.FileHandler(os.path.join(log_dir, log_file))
    file_handler.setLevel(logging.DEBUG)
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[console, file_handler],
    )
    return logging.getLogger(__name__)
