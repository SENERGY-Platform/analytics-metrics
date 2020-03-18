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
import json

import datetime
import re

from flask import request, jsonify
from flask_restx import Namespace, abort, fields, Resource

from server.util import query_influx, get_pipeline_reponse, pipeline_belongs_to_user, get_metrics_for_pipeline

api = Namespace('lastconsumed', description='Check when an operator consumed his last message')

lastconsumed_response = api.model('LastconsumedResponseModel', {
    'datetime': fields.DateTime(description="Datetime of the last consumption. Set to 1970-00-01T00:00:00 if never "
                                            "consumed a message")
})


@api.route('/<string:pipeline_id>/<string:operator_id>')
@api.response(404, 'Could not find pipeline or operator')
@api.response(400, "Pipeline has no metric kafka.process-rate")
class Measurements(Resource):
    def __init__(self, kwargs):
        super().__init__(kwargs)
        self.regex_date = re.compile("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    @api.marshal_with(lastconsumed_response, code=200)
    def get(self, pipeline_id, operator_id):
        user_id = request.headers.get("X-UserID")
        pipeline_reponse = get_pipeline_reponse(pipeline_id, user_id)
        if not pipeline_belongs_to_user(pipeline_reponse, user_id):
            abort(404, "Could not find pipeline with id " + pipeline_id)

        if "kafka.process-rate" not in get_metrics_for_pipeline(pipeline_id):
            abort(400, "Pipeline has no metric kafka.process-rate")

        resp = query_influx(
            "SELECT * FROM \"kafka.process-rate\" WHERE \"operator\" = \'" + operator_id + "\' AND \"value\" > 0 "
                                                                                           "ORDER BY \"time\" DESC "
                                                                                           "LIMIT 1",
            pipeline_id)
        resp_json = json.loads(jsonify(resp.json()).data.decode("utf-8"))
        try:
            datestring = resp_json['results'][0]['series'][0]['values'][0][0]
        except KeyError:
            print("Could not extract datetime. Full response was: " + str(resp_json))

        datestring = self.regex_date.findall(datestring)[0] + "+00:00"
        # influx gives 'Z', but python doesn't understand that
        return {'datetime': datetime.datetime.fromisoformat(datestring)}
