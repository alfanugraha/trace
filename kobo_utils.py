import requests
import json
import pandas as pd

def getFormMetadata(url, title, username, password):
    auth = auth = (username, password)
    response = requests.get(url, auth=auth)

    if(response.status_code == 200):
        print("form metadata request successfull")
        data = response.json()
    else:
        print(f"form metadata request failed with status code: {response.status_code}")
        print(response.text)

    if(len(data) == 0):
        return -1

    for form in data:
        if form["title"] == title:
            metadata = {"title": title,
                        "formid": form["id_string"],
                        "uuid": form["uuid"],
                        "code": form["formid"]}

    return(metadata)

def koboCsvData(url, username, password, additional = ""):
    r = url + '?format=json' + additional
    auth = (username, password)
    response = requests.get(r, auth=auth)

    if(response.status_code == 200):
        data = pd.DataFrame(response.json())
    else: 
        print(f"data request failed with status code: {response.status_code}")
        print(response.text)

    return(data)