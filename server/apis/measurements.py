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

from flask import request
from flask_restx import Namespace, abort, fields, Resource

from server.util import query_influx, get_pipeline_reponse, pipeline_belongs_to_user

api = Namespace('measurements', description='[WIP] Gives information about what operators have logged which metrics for a '
                                            'given pipeline id.')

measurements_model = api.model('MeasurementsResponse', {
    'operators': fields.List(fields.Nested(
        api.model('OperatorMeasurements', {
            'id': fields.String(description='ID of the operator'),
            'name': fields.String(description='Name of the operator'),
            'metrics': fields.List(fields.String)
        }))
    )
})

@api.route('/<string:pipeline_id>')
@api.response(404, 'Could not find pipeline')
class Queries(Resource):
    def __init__(self, kwargs):
        super().__init__(kwargs)

    @api.marshal_with(measurements_model, code=200)
    def get(self, pipeline_id):
        user_id = request.headers.get("X-UserID")
        pipeline_reponse = get_pipeline_reponse(pipeline_id, user_id)
        if not pipeline_belongs_to_user(pipeline_reponse, user_id):
            abort(404, "Could not find pipeline with id " + pipeline_id)
        measurements = {"operators": []}
        for operator in pipeline_reponse.json().get("operators"):
            influx_measurements = query_influx(query="SHOW MEASUREMENTS", operator_id=operator["id"]).json()
            operator_measurements = []
            values = str(influx_measurements["results"][0]["series"][0]["values"])
            values = values.replace('[', '').replace(']', '').replace("'", "").replace(" ", "").split(",")
            for operator_measurement in values:
                operator_measurements.append(operator_measurement)
            measurement = {"id": operator["id"], "name": operator["name"], "metrics": operator_measurements}
            measurements["operators"].append(measurement)

        return measurements
