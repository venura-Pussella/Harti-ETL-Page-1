import requests
from bs4 import BeautifulSoup
import os
from configuration.configuration import pdf_output_directory, pdf_source_url

def get_latest_pdf_link(pdf_source): 
    response = requests.get(pdf_source)
    soup = BeautifulSoup(response.content, 'html.parser')
    pdf_links = soup.find_all('a', href=True)
    
    latest_pdf_link = None
    for link in pdf_links:
        if '.pdf' in link['href']:
            latest_pdf_link = link['href']
            break
    
    return latest_pdf_link

def download_pdf(pdf_url, output_dir, pdf_filename):
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Construct the full output path
    output_path = os.path.join(output_dir, pdf_filename)
    
    # Download and save the PDF
    response = requests.get(pdf_url)
    with open(output_path, 'wb') as f:
        f.write(response.content)

    print(f"PDF downloaded and saved to {output_path}")

def extract_date_from_link(pdf_link):
    date = pdf_link.split('/')[-1].split('.')[0]
    return date

def extract_latest_pdf(url, output_dir):
    latest_pdf_link = get_latest_pdf_link(url)
    if latest_pdf_link:
        pdf_url = f'https://www.harti.gov.lk{latest_pdf_link}'
        date = extract_date_from_link(latest_pdf_link)
        pdf_filename = f"{date}.pdf"
        download_pdf(pdf_url, output_dir, pdf_filename)
        return pdf_filename
    else:
        print('No PDF link found.')
        return None

if __name__ == "__main__":
    
    extract_latest_pdf(pdf_source_url, pdf_output_directory)


