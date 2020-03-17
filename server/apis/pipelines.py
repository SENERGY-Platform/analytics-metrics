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
import requests
from flask import request
from flask_restx import Namespace, abort, fields, Resource
import os

from requests.auth import HTTPBasicAuth

from server.util import called_from_cluster

api = Namespace('pipelines', description='Create and delete databases for pipelines')

pipelines_model = api.model('PipelinesResponse', {
    'database': fields.String(description='ID of the database'),
    'username': fields.String(description='username for auth'),
    'password': fields.String(descirption='Password for auth'),
    'url': fields.String(description="URL of the influx db instance"),
    'interval': fields.String(description="Number of seconds between metric updates")
})

@api.route('/<string:pipeline_id>')
class Pipelines(Resource):
    def __init__(self, kwargs):
        super().__init__(kwargs)

    @api.marshal_with(pipelines_model, code=201)
    def post(self, pipeline_id):
        if not called_from_cluster(request):
            abort(401, "Must not be called from outside")
        req = createInflux(pipeline_id)
        if req.status_code != 200:
            abort(req.status_code, req.json())

        response = {
            'database': pipeline_id,
            'username': os.environ['INFLUX_DB_USER'],
            'password': os.environ['INFLUX_DB_PASSWORD'],
            'url': os.environ['INFLUX_DB_URL'],
            'interval': os.environ['METRICS_INTERVAL']
        }
        return response, 201

    def delete(self, pipeline_id):
        if not called_from_cluster(request):
            abort(401, "Must not be called from outside")
        req = deleteInflux(pipeline_id)
        if req.status_code != 200:
            abort(req.status_code, req.json())

        return "ok", 200


def createInflux(id):
    print("Creating db: " + id)
    url = "{influx_db_url}/query".format(influx_db_url=os.environ["INFLUX_DB_URL"])
    query = "CREATE DATABASE \"" + id + "\""
    query = {"q": query}
    response = requests.post(url, data=query,
                             auth=HTTPBasicAuth(os.environ['INFLUX_DB_USER'], os.environ['INFLUX_DB_PASSWORD']))

    return response

def deleteInflux(id):
    print("Deleting db: " + id)
    url = "{influx_db_url}/query".format(influx_db_url=os.environ["INFLUX_DB_URL"])
    query = "DROP DATABASE \"" + id + "\""
    query = {"q": query}
    response = requests.post(url, data=query,
                             auth=HTTPBasicAuth(os.environ['INFLUX_DB_USER'], os.environ['INFLUX_DB_PASSWORD']))

    return response

