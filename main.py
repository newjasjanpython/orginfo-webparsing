import requests
import bs4
import pandas as pd
import os
import pickle


STARTING_PAGE = 1
ENDING_PAGE = 5


def get_list_of_pages(number):
    url = ""
    content = requests.get(f"https://orginfo.uz/uz/search/organizations/?q=fermer%20xo%27jaligi&page={number}&sort=active").text
    soup = bs4.BeautifulSoup(content, 'lxml')
    elements = soup.find_all('a', class_="text-decoration-none og-card")
    return [i['href'] for i in elements]


def get_number_of_page(page):
    content = requests.get(f"https://orginfo.uz{page}").text
    soup = bs4.BeautifulSoup(content, 'lxml')

    data = {"name": " ".join(soup.find("h1", class_="h1-seo").text.split())}
    rows = soup.find_all('div', class_="row")
    for i in rows:
        name = " ".join(i.find_all('div')[0].text.split())
        if name in ["Telefon raqami", "Manzili", "Elektron pochta", "IFUT"]:
            value = " ".join(i.find_all('div')[1].text.split())
            data[name] = value

    return data


def main():
    try:
        with open('links.pkl', 'rb') as file:
            links = pickle.load(file)
    except:
        links = []
    try:
        with open('data.pkl', 'rb') as file:
            data = pickle.load(file)
    except:
        data = []

    print("Starting to scrape", flush=True)
    continue_page = STARTING_PAGE
    if links:
        continue_page += len(links)//10
    for i in range(continue_page, ENDING_PAGE+1):
        page_links = get_list_of_pages(i)
        links.extend(page_links)
        print("\rFetched", len(links), "from", ENDING_PAGE * 10, end="", flush=True)
        with open('links.pkl', 'wb') as file:
            pickle.dump(links, file)

    print("\n\nStarting main process")
    number = len(data)
    for link in links:
        number += 1
        print("\nFetching data number", number, "of", len(links), link, end="", flush="")
        value = get_number_of_page(link)
        print("\rFetched data", number, "of", len(links), "URL: ", link, end="", flush="")
        data.append(value)
        with open('data.pkl', 'wb') as file:
            pickle.dump(data, file)

    print()
    print("Finished")

    df = pd.DataFrame(data)
    print(df.head())
    df.to_excel('data.xlsx')

if __name__ == '__main__':
    main()

