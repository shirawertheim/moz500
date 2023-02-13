import hashlib
import json
from pprint import pprint

import requests
from bs4 import BeautifulSoup
from celery import Celery
from celery import group

NUMBER_OF_DOMAINS = 20
URL = "https://moz.com/top500/"
HEADER = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
app = Celery('tasks', broker='pyamqp://guest@localhost//', backend='rpc://')
site_list = []


# main function
def initialize():
    global site_list
    domains = crawl()
    site_list = initialize_list_domains(domains)
    write_to_file(site_list)

# ******** part A - extract the domains ********


# The function returns array of strings represent the first 20 domains in the Moz Top 500 sites rank.
def crawl():
    print("crawl:")
    url = URL
    headers = HEADER

    html = get_html(url, headers)
    soup = BeautifulSoup(html, 'html.parser')
    domains = extract_domains(soup)

    print("Those are the first 20 domains in the Moz Top 500 sites rank: ")
    pprint(domains)
    return domains


# The function sends GET request to the MOZ url, and returns it content
def get_html(url, headers):
    try:
        response = requests.get(url, headers=headers)
        return response.content
    except Exception as e:
        print(e)

    return ''


# The function extracts the html content through the tags the correct URLS.
# The loop count depends on the NUMBER_OF_DOMAINS
def extract_domains(soup):
    domains = []
    for i, tr in enumerate(soup.find_all("tr")):
        if len(domains) == NUMBER_OF_DOMAINS:
            break
        if tr.find_all("td"):
            domain = tr.find_all("td")[1].text
            domains.append("https://" + domain)

    return domains


# ******** part B - create object domains ********


# Loop that iterates all the domains, and for each domain initialize an object contains information about it.
def initialize_list_domains(domains):
    print("initialize_list_domains:")
    result_list = []
    result_list.append((group(create_domain_obj.delay(obj).get() for obj in domains)))
    # for obj in domains:
    #     result = create_domain_obj(obj)
    #     result_list.append(result)

    print("Finished to initialize all objects.")
    return result_list


# Send GET request to the domain, and if it works (response of '200'),
# The method returns an object that consist 3 parameters: title, links and favicon_hash.
@app.task
def create_domain_obj(url):
    data = {"title": "", "links": [], "favicon_hash": ""}
    try:
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            extract_title(data, soup)
            extract_links(data, soup)
            extract_favicon(data, url)

    except Exception as e:
        data["title"] = "Title not found"
        data["links"] = []
        data["favicon_hash"] = "Favicon not found"
    return data


# Extract the title of the website
def extract_title(data, soup):
    # Extract the title
    title = soup.title.string if soup.title else "Title not found"
    data["title"] = title


# Extract the links of the website
def extract_links(data, soup):
    links = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href:
            links.append(href)
    data["links"] = links


# Extract the hash of the website's favicon
def extract_favicon(data, url):
    favicon_url = url + "/favicon.ico"
    favicon = requests.get(favicon_url)
    favicon_hash = hashlib.md5(
        favicon.content).hexdigest() if favicon.status_code == 200 else "Favicon not found"
    data["favicon_hash"] = favicon_hash


@app.task
def append_to_list(result_list, result):
    return result_list


# ******** part C - write the objects to a json file ********

# The function gets all the jsons represent the object, and adds it to the jsonfile
def write_to_file(site_list):
    print("Wrting to the json file")
    with open("websites_data.json", "w") as file:
        json.dump(site_list, file)


# ******** main ********

if __name__ == '__main__':
    print('Starting scraping')
    initialize()
    print('Finished scraping')

