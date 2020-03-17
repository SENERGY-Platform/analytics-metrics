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

api = Namespace('measurements', description='Gives information about what operators have logged which metrics for a '
                                            'given pipeline id.')

measurements_model = api.model('MeasurementsResponse', {
    'id': fields.String,
    'metrics': fields.List(fields.String)
})

measurements_model_multi = api.model('MeasurementsResponseMulti', {
    'list': fields.List(fields.Nested(measurements_model))
})

measurements_model_multi_request = api.model('MeasurementsRequestMulti', {
    'ids': fields.List(fields.String)
})


@api.route('/<string:pipeline_id>')
@api.response(404, 'Could not find pipeline')
class Measurements(Resource):
    def __init__(self, kwargs):
        super().__init__(kwargs)

    @api.marshal_with(measurements_model, code=200)
    def get(self, pipeline_id):
        user_id = request.headers.get("X-UserID")
        pipeline_reponse = get_pipeline_reponse(pipeline_id, user_id)
        if not pipeline_belongs_to_user(pipeline_reponse, user_id):
            abort(404, "Could not find pipeline with id " + pipeline_id)

        return {'id': pipeline_id, 'metrics': get_metrics_for_pipeline(pipeline_id)}

@api.route('/')
@api.response(404, 'Could not find pipeline')
class MeasurementsMulti(Resource):
    def __init__(self, kwargs):
        super().__init__(kwargs)

    @api.expect(measurements_model_multi_request)
    @api.marshal_with(measurements_model_multi, code=200)
    def post(self):
        user_id = request.headers.get("X-UserID")

        ids = request.get_json()["ids"]
        response = {'list': []}
        for id in ids:
            pipeline_reponse = get_pipeline_reponse(id, user_id)
            if not pipeline_belongs_to_user(pipeline_reponse, user_id):
                abort(404, "Could not find pipeline with id " + id)
            metrics = {'id': id, 'metrics': get_metrics_for_pipeline(id)}
            response['list'].append(metrics)

        return response

def get_metrics_for_pipeline(id):
    influx_measurements = query_influx(query="SHOW MEASUREMENTS", pipeline_id=id).json()
    metrics = []
    values = str(influx_measurements["results"][0]["series"][0]["values"])
    values = values.replace('[', '').replace(']', '').replace("'", "").replace(" ", "").split(",")
    for operator_measurement in values:
        metrics.append(operator_measurement)

    return metrics