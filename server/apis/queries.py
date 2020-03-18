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
import json
import hashlib
import re
from flask import request, jsonify
from flask_restx import Namespace, abort, Resource, fields
from server.util import query_influx, pipeline_belongs_to_user, get_pipeline_reponse
import pandas as pd

api = Namespace('queries', description="Retrieve metrics")

request_model = api.model("QueriesRequestModel", {
    "time": fields.Nested(api.model("QueriesTimeModel", {
        "last": fields.String(description="Request data from the last x ns|u|µ|ms|s|m|h|d|w; Set either 'last' OR "
                                          "'start' AND 'end'"),
        "start": fields.String(description="Request data with this start date. Set either 'last' OR 'start' AND "
                                           "'end'. Format: \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|(\+|-)\d+("
                                           ":\d+)?)"),
        "end": fields.String(
            description="Request data with this start date. Set either 'last' OR 'start' AND 'end'. Format: \d{4}-\d{"
                        "2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|(\+|-)\d+(:\d+)?)")
    })),
    "group": fields.Nested(api.model("QueriesGroupModel", {
        "time": fields.String(description="Group results by time. Format: \d+(ns|u|µ|ms|s|m|h|d|w)"),
        "type": fields.String(description="Aggregation formula. Allowed are mean, sum, count, median")
    })),
    "queries": fields.List(fields.Nested(api.model("QueriesQueryModel", {
        "pipeline": fields.String(description="Pipeline ID"),
        "operator": fields.String(description="Operator ID"),
        "fields": fields.List(fields.Nested(api.model("QueriesQueryFieldModel", {
            "metric": fields.String(description="Metric to retrieve. Check possible metrics for pipeline with /measurements")
        })))
    }))),
    "limit": fields.Integer(description="Limit number of results to latest x results")
})

response_model = api.model("QueriesResponseModel", {
    "results": fields.List(fields.Nested(api.model("QueriesResultsModel", {
        "series": fields.List(fields.Nested(api.model("QueriesSeriesModel", {
            "columns": fields.List(fields.String(description="Columns, map these to values")),
            "name": fields.String(description="Name of the query"),
            "values": fields.List(fields.List(fields.Raw(description="Actual value, can be Float or String")))
        })))
    })))
})



@api.route('')
@api.response(400, 'Bad request')
class Queries(Resource):
    def __init__(self, kwargs):
        super().__init__(kwargs)
        self.regex_duration = re.compile("\d+(ns|u|µ|ms|s|m|h|d|w)")
        self.regex_math = re.compile("(\+|-|\*|/)\d+((\.|,)\d+)?")
        self.regex_date = re.compile("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|(\+|-)\d+(:\d+)?)")

    @api.expect(request_model)
    @api.marshal_with(response_model)
    def post(self):
        try:
            body = json.loads(request.data.decode("utf-8"))
        except Exception:
            abort(400, "Invalid json")
        try:
            queries = body['queries']
        except KeyError:
            abort(400, "Missing queries object")

        user_id = request.headers.get("X-UserID")

        try:
            grouptime = body['group']['time'].replace(" ", "")
            grouptype = body['group']['type'].replace(" ", "")
            if not (grouptype == "mean" or grouptype == "sum" or grouptype == "count" or grouptype == "median"):
                abort(400, "Unsupported type " + grouptype)
            if self.regex_duration.fullmatch(grouptime) is None:
                abort(400, "Invalid duration " + grouptime)
            use_groups = True
        except KeyError:
            print("Request has no correctly defined group")
            use_groups = False

        querystrings = {}
        columns = []
        operators = []
        for i in range(len(queries)):
            columns.append([])
            columns[i].append("time")
            try:
                pipeline = queries[i]['pipeline']
            except KeyError:
                abort(400, "Missing pipeline for query " + str(i))
            try:
                operator = queries[i]["operator"]
                operators.append(operator)
            except KeyError:
                abort(400, "Missing operator for pipeline " + pipeline)

            # auth check
            try:
                check = pipeline_belongs_to_user(get_pipeline_reponse(pipeline, user_id), user_id)
            except Exception as e:
                print(str(e))
                abort(502, str(e))
            if not type(check) is bool:
                abort(400, check)
            if not check:
                abort(403, jsonify({"error": "missing authorization for accessing measurement"}))

            try:
                fields = queries[i]['fields']
            except KeyError:
                abort(400, "Missing fields for pipeline " + pipeline)
            querystring = "SELECT "
            if use_groups:
                querystring += grouptype + "(value) AS value "
            else:
                querystring += "value "
            querystring += "FROM "
            for j in range(len(fields)):
                try:
                    metric = queries[i]['fields'][j]["metric"]
                except KeyError:
                    abort(400,
                          "Missing metric for field " + str(j) + ", pipeline/operator " + pipeline + "/" + operator)
                columns[i].append(pipeline + ":" + operator + ":" + metric)
                querystring += "\"" + metric + "\""
                if j < len(fields) - 1:
                    querystring += ", "
                else:
                    querystring += " "

            querystring += "WHERE \"operator\" = \'" + operator + "\' "

            try:
                last = body["time"]["last"].replace(" ", "")
                if self.regex_duration.fullmatch(last) is None:
                    abort(400, "Invalid duration " + last)
                querystring += "AND time > now() - " + last
                try:
                    time_start = body["time"]["start"]
                    abort(400, "You supplied 'time.last' and time.start")
                except KeyError:
                    pass
                try:
                    time_end = body["time"]["end"]
                    abort(400, "You supplied 'time.last' and time.end")
                except KeyError:
                    pass
            except KeyError:
                try:
                    time_end = body["time"]["end"].replace(" ", "").replace("'", "")
                    time_start = body["time"]["start"].replace(" ", "").replace("'", "")
                    if self.regex_date.fullmatch(time_start) is None:
                        abort(400, "Invalid 'time.start': " + time_start)
                    if self.regex_date.fullmatch(time_end) is None:
                        abort(400, "Invalid 'time.end': " + time_end)
                    querystring += "AND time < '" + time_end + "' AND time > '" + time_start + "'"
                except KeyError:
                    abort(400, "Missing time parameter")

            if use_groups:
                querystring += " GROUP BY time(" + grouptime + ")"
            try:
                limit = int(body["limit"])
                querystring += " ORDER BY \"time\" DESC LIMIT " + str(limit)
            except KeyError:
                pass
            except Exception:
                abort(400, "Can't parse limit")

            querystrings[i] = {"db": pipeline, "query": querystring}

        df = []
        for i in querystrings:
            print("Query " + str(i) + ": " + querystrings[i]["query"] + ", db: " + querystrings[i]["db"])
            try:
                influxdata = query_influx(querystrings[i]["query"], querystrings[i]["db"])
            except Exception as e:
                print(str(e))
                abort(502, str(e))

            influxdata = jsonify(influxdata.json())
            df_current = dataframes(querystrings[i]["db"], operators[i], influxdata)
            if len(df_current) == 0:
                print("Got empty results for query " + str(i))
                print("Adding empty columns " + str(columns[i]))
                df_current.append(pd.DataFrame(columns=columns[i]))
            for df_single in df_current:
                df.append(df_single)
        hasher = hashlib.sha256()
        hasher.update(request.data)
        return response_from_dfs(df, "metricsquery." + str(hasher.hexdigest()))


def dataframes(pipeline, operator, response):
    if 'error' in response.json:
        raise Exception(response.json)
    df = []
    try:
        print(str(response.data.decode("utf-8")))
        seriess = json.loads(response.data.decode("utf-8"))['results'][0]['series']
    except KeyError as ke:
        print("Got empty results for query with pipeline/operator " + pipeline + "/" + operator)
        return df
    for series in seriess:
        columns = series["columns"]
        values = series['values']

        for i in range(len(columns)):
            if columns[i] != "time":
                columns[i] = pipeline + ":" + operator + ":" + series["name"]
        df.append(pd.DataFrame.from_records(values, columns=columns))
    return df


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
            if type(ij) is float and math.isnan(ij):
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
