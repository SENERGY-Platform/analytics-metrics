"""
   Copyright 2020 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import math
import os
import requests
import json
from requests.auth import HTTPBasicAuth
import pandas as pd


def query_influx(query, pipeline_id, params=None):
    query = escape(query)
    url = "{influx_db_url}/query".format(influx_db_url=os.environ["INFLUX_DB_URL"])
    if params:
        params = escape(params)
        response = requests.post(url, params=f'db={pipeline_id}&q={query}&params={str(params)}',
                                 auth=HTTPBasicAuth(os.environ['INFLUX_DB_USER'], os.environ['INFLUX_DB_PASSWORD']))
        print("Request: " + url + "?db=" + pipeline_id + "&q=" + query + "&params=" + str(params))
        return response
    else:
        query = {"q": query}
        response = requests.post(url, params="db=" + pipeline_id, data=query,
                                 auth=HTTPBasicAuth(os.environ['INFLUX_DB_USER'], os.environ['INFLUX_DB_PASSWORD']))
        print("Request: " + url + "?db=" + pipeline_id + "&q=" + query["q"])
        return response


def escape(value):
    """
    Escape a string, which can be user input. Therefore quotes have to be escaped and then wrapped into own quotes.
    """
    escape_map = [('"', '\"'), ("'", "\'")]
    for escape_pair in escape_map:
        value = value.replace(escape_pair[0], escape_pair[1])
    return value


def get_pipeline_reponse(id, user_id):
    response = requests.get(os.environ["PIPELINE_URL"] + '/' + id, headers={'X-UserId': user_id})
    if not response.status_code == 200:
        print("pipeline-service responded with " + str(response.status_code) + " for pipeline " + str(id))
    return response


def get_pipelines_reponse(user_id):
    response = requests.get(os.environ["PIPELINE_URL"] + '?order=createdat:desc', headers={'X-UserId': user_id})
    if not response.status_code == 200:
        print("pipeline-service responded with " + str(response.status_code) + " for pipeline " + str(id))
    return response


def pipeline_belongs_to_user(pipeline_response, user_id):
    if pipeline_response.status_code == 200:
        result = pipeline_response.json()
    else:
        return False

    if user_id == result.get("UserId"):
        return True
    print("pipeline does not belong to user")
    return False


def get_operators_of_pipeline(pipeline_reponse):
    operators = []
    for operator in pipeline_reponse.json().get("operators"):
        operators.append(operator)
    return operators


def called_from_cluster(request):
    if request.headers.get("X-UserID", "none") == "none":
        return True
    return False

def get_metrics_for_pipeline(id):
    influx_measurements = query_influx(query="SHOW MEASUREMENTS", pipeline_id=id).json()
    metrics = []
    values = str(influx_measurements["results"][0]["series"][0]["values"])
    values = values.replace('[', '').replace(']', '').replace("'", "").replace(" ", "").split(",")
    for operator_measurement in values:
        metrics.append(operator_measurement)

    return metrics