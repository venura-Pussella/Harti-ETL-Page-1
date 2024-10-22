import os
import sys
import logging

logging_str = "[%(asctime)s: %(levelname)s: %(module)s: %(message)s]"

log_dir = "logs"

log_filepath = os.path.join(log_dir, "running_log.log")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level = logging.INFO,
    format = logging_str,

    handlers=[
        logging.FileHandler(log_filepath), #Handling issues in the data set 
        logging.StreamHandler(sys.stdout)    #Handling issues in the in the time stamp
    ]
)

logger = logging.getLogger("Harti_PDF_Scraper_Page_1_ETL")

# set azure logger to warnings only
logger2 = logging.getLogger('azure')
logger2.setLevel(logging.WARNING)