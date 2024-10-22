# Text Extractor all is for batch processing PDFs only
import io
import requests
import pdfplumber
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def get_all_pdf_links(pdf_source):
    response = requests.get(pdf_source)
    soup = BeautifulSoup(response.content, 'html.parser')
    pdf_links = soup.find_all('a', href=True)
    
    all_pdf_links: list[str] = []
    for link in pdf_links:
        if '.pdf' in link['href']:
            full_link = urljoin(pdf_source, link['href'])
            all_pdf_links.append(full_link)
    
    return all_pdf_links
            

def download_pdf_as_bytes(pdf_url):
    response = requests.get(pdf_url)
    pdf_bytes = io.BytesIO(response.content)
    return pdf_bytes

def extract_text_from_first_page(pdf_data):
    with pdfplumber.open(pdf_data) as pdf:
        first_page_text = pdf.pages[0].extract_text()
    return first_page_text
