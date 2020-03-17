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


def query_influx(query, operator_id, params=None):
    query = escape(query)
    url = "{influx_db_url}/query".format(influx_db_url=os.environ["INFLUX_DB_URL"])
    if params:
        params = escape(params)
        response = requests.post(url, params=f'db={operator_id}&q={query}&params={str(params)}',
                                 auth=HTTPBasicAuth(os.environ['INFLUX_DB_USER'], os.environ['INFLUX_DB_PASSWORD']))
        print("Request: " + url + "?db=" + operator_id + "&q=" + query + "&params=" + str(params))
        return response
    else:
        query = {"q": query}
        response = requests.post(url, params="db=" + operator_id, data=query,
                                 auth=HTTPBasicAuth(os.environ['INFLUX_DB_USER'], os.environ['INFLUX_DB_PASSWORD']))
        print("Request: " + url + "?db=" + operator_id + "&q=" + query["q"])
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


def response_from_dfs(df, name):
    resp = empty_response()

    if len(df) < 1:
        return resp

    for i in range(1, len(df)):
        df[0] = df[0].merge(df[i], how="outer", on="time")

    df[0].sort_values(axis=0, by='time', inplace=True, ascending=False)

    nump = df[0].to_numpy()
    values = []
    for i in range(nump.shape[0]):
        value = []
        for j in range(nump.shape[1]):
            ij = nump[i][j]
            if type(ij) is not str and math.isnan(ij):
                value.append(None)
            else:
                value.append(ij)
        values.append(value)
    columns = []
    for i in df[0].columns:
        columns.append(i)

    resp['results'][0]['series'][0]['columns'] = columns
    resp['results'][0]['series'][0]['name'] = 'merge.' + name
    resp['results'][0]['series'][0]['values'] = values

    return resp


def empty_response():
    return {
        'results': [
            {
                'series': [
                    {
                        'columns': [],
                        'name': '',
                        'values': []
                    }
                ],
                'statement_id': 0
            }
        ]
    }

def called_from_cluster(request):
    if request.headers.get("X-UserID", "none") == "none":
        return True
    return False

def dataframes(id, response):
    if 'error' in response.json:
        raise Exception(response.json)
    df = []
    try:
        seriess = json.loads(response.data.decode("utf-8"))['results'][0]['series']
    except KeyError as ke:
        print("Got empty results for query with id " + id)
        return df
    for series in seriess:
        columns = series["columns"]
        values = series['values']

        for i in range(len(columns)):
            if columns[i] != "time":
                columns[i] = str(id) + ":" + series["name"]
        df.append(pd.DataFrame.from_records(values, columns=columns))
    return df


class OperatorChecker:
    def __init__(self, userId):
        self.operators = []
        for pipeline in get_pipelines_reponse(userId).json():
            pipeline_response = get_pipeline_reponse(pipeline["id"], userId)
            operators = get_operators_of_pipeline(pipeline_response)
            for operator in operators:
                self.operators.append(operator["id"])

    def user_has_operator(self, operatorId):
        return operatorId in self.operators
