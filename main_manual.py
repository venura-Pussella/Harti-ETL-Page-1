# main.py
import asyncio
import platform
import os
from src import logger
from src.utils.log_utils import send_log
from src.connector.blob import upload_to_blob
from src.connector.cosmos_db import write_harti_data_to_cosmosdb
from src.configuration.configuration import metadata_line1
from src.pipeline1.lists_to_dataframe import create_dataframe
from src.pipeline1.data_transformer import transform_dataframe
from src.pipeline1.text_to_lists import parse_text, get_patterns
from src.pipeline1.metadata_reader import find_line_with_metadata
from src.pipeline1.data_format_converter import dataframe_to_csv_string, convert_dataframe_to_cosmos_format
from src.pipeline1.text_extractor_all import download_pdf_as_bytes, extract_text_from_first_page, get_all_pdf_links
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv()) # read local .env file

STATUS_FILE = 'processed_pdfs.txt'
LINKS_FILE = 'pdf_links.txt'
WEB_SOURCE = 'https://www.harti.gov.lk/index.php/en/market-information/data-food-commodities-bulletin'
container_name = os.getenv('container_name_blob')
az_blob_conn_str = os.getenv('connect_str')

def download_processed_pdfs():
    blob_service_client = BlobServiceClient.from_connection_string(az_blob_conn_str)
    container_client = blob_service_client.get_container_client(container= container_name) 
    with open(file=STATUS_FILE, mode="wb") as download_file:
        download_file.write(container_client.download_blob(STATUS_FILE).readall()) # name in blob is also identical to STATUS_FILE

def upload_processed_pdfs():
    blob_service_client = BlobServiceClient.from_connection_string(az_blob_conn_str)
    container_client = blob_service_client.get_container_client(container= container_name) 
    with open(file=STATUS_FILE, mode="rb") as data:
        blob_client = container_client.upload_blob(name=STATUS_FILE, data=data, overwrite=True) # name in blob is also identical to STATUS_FILE

def load_processed_pdfs():
    """Returns set of all already processed PDF links, by reading STATUS_FILE
    """
    if os.path.exists(STATUS_FILE):
        mySet = set()
        with open(STATUS_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                line = line.rsplit('.pdf')[0] # to get rid of '%20' characters after the pdf name in the link. eg: 'https://www.harti.gov.lk/images/download/market_information/2015/October/daily_23-10-2015.pdf%20%20%20%20%20%20%20%20%20%20'
                if line != '': line += '.pdf'
                mySet.add(line)
    return mySet

def save_processed_pdf(pdf_link: str):
    """Appends STATUS_FILE with the passed in pdf_link

    Args:
        pdf_link (str): _description_
    """
    with open(STATUS_FILE, 'a') as f:
        f.write(f"{pdf_link}\n")

def load_pdf_links_from_file():
    """Returns list of links (as strings) read from LINKS_FILE
    """
    links = []
    if os.path.exists(LINKS_FILE):
        with open(LINKS_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                line = line.rsplit('.pdf')[0] # to get rid of '%20' characters after the pdf name in the link. eg: 'https://www.harti.gov.lk/images/download/market_information/2015/October/daily_23-10-2015.pdf%20%20%20%20%20%20%20%20%20%20'
                if line != '': line += '.pdf'
                links.append(line)
        return links
    else:
        logger.error(f"File {LINKS_FILE} not found.")
        return []

async def process_pdf(pdf_link):
    try:
        logger.info(f"Processing PDF link: {pdf_link}")
        pdf_bytes = download_pdf_as_bytes(pdf_link)
        extracted_text = extract_text_from_first_page(pdf_bytes)

        # Extracted text is split into lines
        extracted_lines = extracted_text.split('\n')

        # Check if metadata line exists
        if find_line_with_metadata(extracted_lines, metadata_line1):
            
            logger.info(">>>> Metadata line found. Proceeding with data processing... <<<<")
            
            # Get patterns
            category_pattern, item_pattern = get_patterns()

            # Parse text to lists
            dates, categories, items, pettah_price_ranges, pettah_averages = parse_text(extracted_lines, category_pattern, item_pattern)

            # Convert lists to DataFrame
            list_to_dataframe = create_dataframe(dates, categories, items, pettah_price_ranges, pettah_averages)

            # Transform DataFrame
            transformed_dataframe = transform_dataframe(list_to_dataframe)

            logger.info(">>>> Data transformation completed <<<<")

            # Convert DataFrame to CSV string
            csv_data, actual_date_str = dataframe_to_csv_string(transformed_dataframe)

            # Upload CSV data to blob
            upload_to_blob(csv_data, actual_date_str)
            logger.info(">>>> CSV data uploaded to blob storage <<<<")

            # Convert DataFrame to Cosmos DB format
            cosmos_db_data = convert_dataframe_to_cosmos_format(transformed_dataframe)

            # Write Cosmos DB data
            await write_harti_data_to_cosmosdb(cosmos_db_data)
            logger.info(">>>> Completion of data ingestion to CosmosDB <<<<")

            # Send success log
            send_log(
                service_type="Container Application - Manual Run",
                application_name="Harti Food Price Collector Page 1",
                project_name="Harti Food Price Prediction",
                project_sub_name="Food Price History",
                azure_hosting_name="ML Services",
                developmental_language="Python",
                description="Sri Lanka Food Prices - Manual Run Containerized Application",
                created_by="BrownsAIseviceTest",
                log_print="Successfully completed data ingestion to Cosmos DB.",
                running_within_minutes=1440,
                error_id=0
                )
            logger.info("Sent success log to function monitoring service.")

            save_processed_pdf(pdf_link)

        else:
            logger.warning("Metadata line not found. Skipping this PDF.")
            save_processed_pdf(pdf_link)

    except Exception as e:
        logger.error(f"Error processing PDF {pdf_link}: {e}")
        save_processed_pdf(pdf_link)

        # Send error log
        send_log(
            service_type="Container Application - Manual Run",
            application_name="Harti Food Price Collector Page 1",
            project_name="Harti Food Price Prediction",
            project_sub_name="Food Price History",
            azure_hosting_name="ML Services",
            developmental_language="Python",
            description="Sri Lanka Food Prices - Manual Run Containerized Application",
            created_by="BrownsAIseviceTest",
            log_print="An error occurred: " + str(e),
            running_within_minutes=1440,
            error_id=1,
            )
        logger.error("Sent error log to function monitoring service.")
        raise

async def main():
    try:
        logger.info(">>>> Starting the data extraction process <<<<")

        # Write all available pdf links on Harti website to LINKS_FILE
        get_all_pdf_links(WEB_SOURCE, LINKS_FILE) 

        # Load PDF links from file
        pdf_links = load_pdf_links_from_file()
        if not pdf_links:
            logger.warning("No PDF links found.")
            return
        
        # Load already processed PDFs
        download_processed_pdfs()
        processed_pdfs = load_processed_pdfs()

        # Loop through each PDF link and process it
        for pdf_link in pdf_links:
            if pdf_link not in processed_pdfs:
                logger.info(f"New PDF link: {pdf_link}")
                await process_pdf(pdf_link)                
            else:
                logger.info(f"Skipping already processed PDF link: {pdf_link}")
        
        logger.info(">>>> Data extraction process completed <<<<")

        # update processed pdf tracker file in blob
        upload_processed_pdfs()

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
 
def run_main():
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

if __name__ == '__main__':
    run_main()
