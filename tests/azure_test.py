import requests
def check_connectivity(url):
    try:
        response = requests.head(url, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(response.headers)
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

# Example usage
check_connectivity('https://www.harti.gov.lk/index.php/en/market-information/data-food-commodities-bulletin')
