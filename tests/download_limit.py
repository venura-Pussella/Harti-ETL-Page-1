import io
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def get_latest_pdf_link(): 
    pdf_source = 'https://www.harti.gov.lk/index.php/en/market-information/data-food-commodities-bulletin'
    response = requests.get(pdf_source)
    soup = BeautifulSoup(response.content, 'html.parser')
    pdf_links = soup.find_all('a', href=True)
    
    latest_pdf_link = None
    for link in pdf_links:
        if '.pdf' in link['href']:
            latest_pdf_link = urljoin(pdf_source, link['href'])  # Construct the full URL
            break
    
    return latest_pdf_link

# Download the latest PDF
for k in range(100):
    latest_pdf_link = get_latest_pdf_link()
    if latest_pdf_link:
        response = requests.get(latest_pdf_link)
        pdf_bytes = io.BytesIO(response.content)
        print(f"count: {k+1}")
        # time.sleep(1)