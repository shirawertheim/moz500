from celery import Celery
import requests
from bs4 import BeautifulSoup
import hashlib
import json
from collections import namedtuple



from celery.schedules import crontab # scheduler
from celery import group
# from tasks import urlopen

NUMBER_OF_DOMAINS=20
# app = Celery('tasks', broker_url='redis://127.0.0.1:6379/1')
app = Celery('tasks', broker='pyamqp://guest@localhost//', backend='rpc://')

# dictionary = []

# app = Celery('tasks')
# app.conf.update(
#     worker_concurrency=5,
#     worker_max_tasks_per_child=100,
#     worker_pool='gevent',
#     task_acks_late=True,
#     task_reject_on_worker_lost=True,
# )


# app = Celery('tasks')
# app.conf.timezone = 'UTC'
# app.conf.beat_schedule = {
#     # executes every 1 minute
#     'scraping-task-one-min': {
#         'task': 'tasks.create_domain_obj',
#         'schedule': crontab(),
#     },
#     # # executes every 15 minutes
#     # 'scraping-task-fifteen-min': {
#     #     'task': 'tasks.hackernews_rss',
#     #     'schedule': crontab(minute='*/15')
#     # },
#     # # executes daily at midnight
#     # 'scraping-task-midnight-daily': {
#     #     'task': 'tasks.hackernews_rss',
#     #     'schedule': crontab(minute=0, hour=0)
#     # }
# }

# app.conf.update(
#     task_acks_late=True,
#     worker_concurrency=5
# )

def initialize():
    print("initialize")
    # app.start(argv=['celery', 'worker', '-c', '20', '-l', 'INFO'])
    domains = crawl()
    # worker = app.Worker(concurrency=5)
    # worker.start()
    initialize_dictionary_domains(domains)
    # write_to_file(dictionary)
    print("hello")



def initialize_dictionary_domains(domains):
    print("initialize_dictionary_domains")
    result_dict = []
    for obj in domains:
        # result = create_domain_obj.apply_async(args=[obj])
        result = create_domain_obj(obj)
        result_dict.append(result)

    return result_dict

# @app.task
# def initialize_dictionary_domains(domains):
#     print("initialize_dictionary_domains")
#     print()
#     # dictionary = []
#     result = group(create_domain_obj(url)
#                    for url in domains).apply_async()
#     for incoming_result in result.iter_native():
#         print(incoming_result)


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
        if len(domains) == NUMBER_OF_DOMAINS:
            break
        if tr.find_all("td"):
            domain = tr.find_all("td")[1].text
            domains.append("https://" + domain)

    return domains



@app.task
def create_domain_obj(url):
    print("create_domain_obj")
    data = ["", [], ""]

    try:
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            # Extract the title
            title = soup.title.string if soup.title else "Title not found"
            data[0] = title

            # Extract the links
            links = []
            for link in soup.find_all("a"):
                href = link.get("href")
                if href:
                    links.append(href)

            data[1] = links

            # Extract the hash of the website's favicon
            favicon_url = url + "/favicon.ico"
            favicon = requests.get(favicon_url)
            favicon_hash = hashlib.md5(
                favicon.content).hexdigest() if favicon.status_code == 200 else "Favicon not found"
            data[2] = favicon_hash

    except Exception as e:
        data[0] = "Title not found"
        data[1] = []
        data[2] = "Favicon not found"
        print(f"An exception occurred: {e}")

    print(data)
    return data


def write_to_file(dictionary):
    with open("websites_data.json", "w") as file:
        json.dump(dictionary, file)


if __name__ == '__main__':
    print('Starting scraping')
    initialize()
    print('Finished scraping')

