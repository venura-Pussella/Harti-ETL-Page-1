# main.py
import asyncio
import platform
import os
import pdfminer
import pdfminer.pdfparser
import logging # for use in Azure functions environment (replace all calls to logger object with python logging class)
from src import logHandling
from src.logHandling import log_messages
from src.utils.log_utils import send_log
from src.connector.blob import upload_to_blob, upload_processed_pdfs, download_processed_pdfs, update_logs
from src.connector.cosmos_db import write_harti_data_to_cosmosdb
from src.configuration.configuration import metadata_line1
from src.pipeline1.lists_to_dataframe import create_dataframe
from src.pipeline1.data_transformer import transform_dataframe
from src.pipeline1.text_to_lists import parse_text, get_patterns
from src.pipeline1.metadata_reader import find_line_with_metadata
from src.pipeline1.data_format_converter import dataframe_to_csv_string, convert_dataframe_to_cosmos_format
from src.pipeline1.text_extractor_all import download_pdf_as_bytes, extract_text_from_first_page, get_all_pdf_links
# from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv()) # read local .env file
from src.configuration.configuration import WEB_SOURCE

container_name = os.getenv('container_name_blob')
az_blob_conn_str = os.getenv('connect_str')

def load_processed_pdfs(status_file_string: str):
    """Expects a string consisting of pdf_link lines, returns it as a set
    """
    mySet = set()
    status_file_lines = status_file_string.rsplit('\n')
    for line in status_file_lines:
        line = line.strip()
        mySet.add(line)
    return mySet

async def process_pdf(pdf_link):
    try:
        logging.info(f"Processing PDF link: {pdf_link}")
        pdf_bytes = download_pdf_as_bytes(pdf_link)
        extracted_text = extract_text_from_first_page(pdf_bytes)

        # Extracted text is split into lines
        extracted_lines = extracted_text.split('\n')

        # Check if metadata line exists
        if find_line_with_metadata(extracted_lines, metadata_line1):
            
            logging.info(">>>> Metadata line found. Proceeding with data processing... <<<<")
            
            # Get patterns
            category_pattern, item_pattern = get_patterns()

            # Parse text to lists
            dates, categories, items, pettah_price_ranges, pettah_averages = parse_text(extracted_lines, category_pattern, item_pattern)

            # Convert lists to DataFrame
            list_to_dataframe = create_dataframe(dates, categories, items, pettah_price_ranges, pettah_averages)

            # Transform DataFrame
            transformed_dataframe = transform_dataframe(list_to_dataframe)

            logging.info(">>>> Data transformation completed <<<<")

            # Convert DataFrame to CSV string
            csv_data, actual_date_str = dataframe_to_csv_string(transformed_dataframe)

            # Upload CSV data to blob
            upload_to_blob(csv_data, actual_date_str)
            logging.info(">>>> CSV data uploaded to blob storage <<<<")

            # Convert DataFrame to Cosmos DB format
            cosmos_db_data = convert_dataframe_to_cosmos_format(transformed_dataframe)

            # Write Cosmos DB data
            await write_harti_data_to_cosmosdb(cosmos_db_data)
            logging.info(">>>> Completion of data ingestion to CosmosDB <<<<")

            # Send success log
            send_log(
                service_type="Azure Functions",
                application_name="Harti Food Price Collector Page 1",
                project_name="Harti Food Price Prediction",
                project_sub_name="Food Price History",
                azure_hosting_name="AI Services",
                developmental_language="Python",
                description="Sri Lanka Food Prices - Azure Functions",
                created_by="BrownsAIsevice",
                log_print="Successfully completed data ingestion to Cosmos DB.",
                running_within_minutes=1440,
                error_id=0
                )
            logging.info("Sent success log to function monitoring service.")


        else:
            logging.warning("Metadata line not found. Skipping this PDF.")

    except Exception as e:
        logging.error(f"Error processing PDF {pdf_link}: {e}")

        # Send error log
        send_log(
            service_type="Azure Functions",
            application_name="Harti Food Price Collector Page 1",
            project_name="Harti Food Price Prediction",
            project_sub_name="Food Price History",
            azure_hosting_name="AI Services",
            developmental_language="Python",
            description="Sri Lanka Food Prices - Azure Functions",
            created_by="BrownsAIsevice",
            log_print="An error occurred: " + str(e),
            running_within_minutes=1440,
            error_id=1,
            )
        logging.error("Sent error log to function monitoring service.")
        raise

async def main():
    try:
        logging.info(">>>> Starting the data extraction process <<<<")

        # Get list of all available pdf links from Harti website
        pdf_links = get_all_pdf_links(WEB_SOURCE) 

        if not pdf_links:
            logging.warning("No PDF links found.")
            return
        
        # Load already processed PDFs
        status_file_string = download_processed_pdfs()
        processed_pdfs = load_processed_pdfs(status_file_string)

        # Loop through each PDF link and process it. After processing, add the link to the set of processed pdf links.
        for pdf_link in pdf_links:
            if pdf_link not in processed_pdfs:
                logging.info(f"New PDF link: {pdf_link}")
                try:
                    await process_pdf(pdf_link)   
                except pdfminer.pdfparser.PDFSyntaxError:
                    logging.error(f"PDF Syntax Error{pdf_link}")
                processed_pdfs.add(pdf_link)             
            else:
                logging.info(f"Skipping already processed PDF link: {pdf_link}")
        
        logging.info(">>>> Data extraction process completed <<<<")

        # update processed pdf tracker file in blob
        processed_pdfs_string = ''
        for link in processed_pdfs:
            processed_pdfs_string += link
            processed_pdfs_string += '\n'
        upload_processed_pdfs(processed_pdfs_string)
        logging.info(">>>> Processed PDF Tracker uploaded to blob <<<<")

    except Exception as e:
        logging.error(f"Error in main execution: {e}")
 
def run_main():
    logging.info('started run_main()')
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

    try: update_logs(log_messages)
    except Exception as e: logging.ERROR(f'Exception when updating logs: {e}')


# run_main() # only for local testing