# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def access_moz():
    import requests

    url = "https://api.moz.com/metrics/top-500/seo"

    headers = {
        "Authorization": "Bearer [YOUR API KEY HERE]"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        top_sites = [item['url'] for item in data['data']]
        print(top_sites)
    else:
        print("Error accessing the Moz API.")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    access_moz()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
