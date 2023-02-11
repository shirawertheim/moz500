from celery import Celery
import requests
from bs4 import BeautifulSoup
import hashlib

app = Celery('tasks', broker_url='redis://127.0.0.1:6379/1')


def initialize():
    print("initialize")
    domains = crawl()
    dict = initialize_dictionary_domains(domains)
    print("hello")


def initialize_dictionary_domains(domains):
    print("initialize_dictionary_domains")
    print()
    dictionary = []
    for domain in domains:
        object = create_domain_obj(domain)
        print(object)
        dictionary.append(object)

    return dictionary


@app.task
def crawl():
    print("crawl")

    url = "https://moz.com/top500/"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    html = get_html(url, headers)
    soup = BeautifulSoup(html, 'html.parser')
    domains = extract_domains(soup)
    print(domains)
    return domains


def get_html(url,headers):
    try:
        response = requests.get(url, headers=headers)
        return response.content
    except Exception as e:
        print(e)

    return ''


def extract_domains(soup):
    domains = []
    for i, tr in enumerate(soup.find_all("tr")):
        if len(domains) == 20:
            break
        if tr.find_all("td"):
            domain = tr.find_all("td")[1].text
            domains.append("https://" + domain)

    return domains





def create_domain_obj(url):
    print("create_domain_obj")
    data = {"title": "", "links": [], "favicon_hash": ""}

    try:
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")


            # Extract the title
            title = soup.title.string if soup.title else "Title not found"
            data["title"] = title

            # Extract the links
            links = []
            for link in soup.find_all("a"):
                href = link.get("href")
                if href:
                    links.append(href)


            data["links"] = links

            # Extract the hash of the website's favicon
            favicon_url = url + "/favicon.ico"
            favicon = requests.get(favicon_url)
            favicon_hash = hashlib.md5(favicon.content).hexdigest() if favicon.status_code == 200 else "Favicon not found"
            data["favicon_hash"] = favicon_hash

    except Exception as e:
        data["title"] = "Title not found"
        data["links"] = []
        data["favicon_hash"] = "Favicon not found"
        print(f"An exception occurred: {e}")

    return data



if __name__ == '__main__':
    print('Starting scraping')
    domain = "https://youtube.com"
    initialize()
    print('Finished scraping')

