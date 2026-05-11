import logging
import os


def setup_logging(log_dir="logs", log_file="company_ids.log"):
    os.makedirs(log_dir, exist_ok=True)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    file_handler = logging.FileHandler(os.path.join(log_dir, log_file))
    file_handler.setLevel(logging.DEBUG)
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[console, file_handler],
    )
    return logging.getLogger(__name__)
