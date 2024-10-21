import io
import requests
import pdfplumber
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def get_latest_pdf_link(pdf_source): 
    response = requests.get(pdf_source)
    soup = BeautifulSoup(response.content, 'html.parser')
    pdf_links = soup.find_all('a', href=True)
    
    latest_pdf_link = None
    for link in pdf_links:
        if '.pdf' in link['href']:
            latest_pdf_link = urljoin(pdf_source, link['href'])  # Construct the full URL
            break
    
    return latest_pdf_link

def download_pdf_as_bytes(pdf_url):
    # Download the PDF
    response = requests.get(pdf_url)
    pdf_bytes = io.BytesIO(response.content)

    return pdf_bytes

def extract_text_from_first_page(pdf_data):
    # Read PDF file
    with pdfplumber.open(pdf_data) as pdf:
        # Extract text from the first page only
        
        first_page_text = pdf.pages[0].extract_text()
       
    return first_page_text