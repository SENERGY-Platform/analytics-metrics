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

import logging
import os

from flask import Flask
from flask_cors import CORS
from flask_restx import Resource
from flask_restx import Api


from server.apis.queries import api as queries
from server.apis.measurements import api as measurements
from server.apis.pipelines import api as pipelines
from server.apis.lastconsumed import api as lastconsumed



application = Flask("analytics-metrics")
CORS(application)
api = Api(
    application,
    title='analytics-metrics',
    version='0.1'
)

api.add_namespace(queries)
api.add_namespace(measurements)
api.add_namespace(pipelines)
api.add_namespace(lastconsumed)

@api.route('/doc')
class Docs(Resource):
    def __init__(self, kwargs):
        super().__init__(kwargs)

    def get(self):
        return api.__schema__

application.logger.addHandler(logging.StreamHandler())
application.logger.setLevel(logging.INFO)


if __name__ == '__main__':
    try:
        if os.environ["DEBUG"] == "true":
            debug = True
        else:
            debug = False
    except KeyError:
        debug = False
    if debug:
        application.run(debug=True, host='0.0.0.0')
    else:
        application.run(debug=False, host='0.0.0.0')
