import requests
import bs4
import pandas as pd
import pickle
import multiprocessing
from tqdm import tqdm

STARTING_PAGE = 1
ENDING_PAGE = 1000
NUMBER_PROCESSES = 16

def get_list_of_pages(page_number):
    """Fetch organization page links from a single listing page."""
    try:
        response = requests.get(
            f"https://orginfo.uz/uz/search/organizations/?q=fermer%20xo%27jaligi&page={page_number}&sort=active",
            timeout=10
        )
        response.raise_for_status()
        soup = bs4.BeautifulSoup(response.text, 'lxml')
        elements = soup.find_all('a', class_="text-decoration-none og-card")
        return [i['href'] for i in elements]
    except Exception as e:
        print(f"[Error] Page {page_number}: {e}")
        return []

def get_number_of_page(link):
    """Fetch details of a single organization."""
    try:
        response = requests.get(f"https://orginfo.uz{link}", timeout=10)
        response.raise_for_status()
        soup = bs4.BeautifulSoup(response.text, 'lxml')
        data = {"name": " ".join(soup.find("h1", class_="h1-seo").text.strip().split())}
        rows = soup.find_all('div', class_="row")

        for row in rows:
            key = " ".join(row.find_all('div')[0].text.strip().split())
            if key in ["Telefon raqami", "Manzili", "Elektron pochta", "IFUT"]:
                value = " ".join(row.find_all('div')[1].text.strip().split())
                data[key] = value
        return data
    except Exception as e:
        print(f"[Error] Link {link}: {e}")
        return {}

def save_pickle(filename, data):
    with open(filename, 'wb') as file:
        pickle.dump(data, file)

def load_pickle(filename):
    try:
        with open(filename, 'rb') as file:
            return pickle.load(file)
    except FileNotFoundError:
        return []

def fetch_links(pages):
    """Parallel fetching of all links."""
    with multiprocessing.Pool(NUMBER_PROCESSES) as pool:
        results = list(tqdm(pool.imap_unordered(get_list_of_pages, pages), total=len(pages), desc="Fetching Links"))
    # Flatten the list of lists
    return [link for sublist in results for link in sublist]

def fetch_data(links):
    """Parallel fetching of all organization details."""
    with multiprocessing.Pool(NUMBER_PROCESSES) as pool:
        return list(tqdm(pool.imap_unordered(get_number_of_page, links), total=len(links), desc="Fetching Data"))

def main():
    print("🔎 Loading previously saved data...")
    links = load_pickle('links.pkl')
    data = load_pickle('data.pkl')

    # Step 1: Fetch Links
    if not links or len(links) < (ENDING_PAGE - STARTING_PAGE + 1) * 10:
        print("🚀 Starting link scraping...")
        pages = range(STARTING_PAGE, ENDING_PAGE + 1)
        new_links = fetch_links(pages)
        links.extend(new_links)
        save_pickle('links.pkl', links)
        print(f"✅ Collected {len(links)} links.")
    else:
        print(f"✅ Loaded {len(links)} existing links.")

    # Step 2: Fetch Data
    print("\n📦 Starting data scraping...")
    pending_links = links[len(data):]
    if pending_links:
        new_data = fetch_data(pending_links)
        data.extend(new_data)
        save_pickle('data.pkl', data)
        print(f"✅ Fetched data for {len(data)} organizations.")
    else:
        print("✅ All data already scraped.")

    # Step 3: Save to Excel
    print("\n💾 Saving data to Excel...")
    df = pd.DataFrame(data)
    df.to_excel('data.xlsx', index=False)
    print("🎉 Data saved to 'data.xlsx'.")

if __name__ == '__main__':
    multiprocessing.freeze_support()  # For Windows compatibility
    main()
