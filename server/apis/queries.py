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

'''
Structure of POST Body:
{
  "time": {
    "last": "1d" //either last or (start and end)
    "start": "2015-08-18T00:00:00Z",
    "end": "2015-08-18T00:30:00Z",
  },
  "group": {
    "time": "12m",
    "type": "count,sum,mean,median" //default = mean
  },
  "queries": [
    {
      "pipeline": "" //pipeline -> db
      "operator": "", // 'pipe:operator' -> db
      "fields": {
        "metric": "", // metric -> table
      }
    }
  ],
  "limit": 100 // optional, will return values for all queries when available
}

TODO: Math
'''

import json
import hashlib
import re

from flask import request, jsonify
from flask_restx import Namespace, abort, Resource

from server.util import query_influx, dataframes, response_from_dfs, pipeline_belongs_to_user, get_pipeline_reponse

import pandas as pd

api = Namespace('queries', description="Retrieve metrics")


@api.route('')
@api.response(400, 'Bad request')
class Queries(Resource):
    def __init__(self, kwargs):
        super().__init__(kwargs)
        self.regex_duration = re.compile("\d+(ns|u|µ|ms|s|m|h|d|w)")
        self.regex_math = re.compile("(\+|-|\*|/)\d+((\.|,)\d+)?")
        self.regex_date = re.compile("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|(\+|-)\d+(:\d+)?)")

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
        for i in range(len(queries)):
            columns.append([])
            columns[i].append("time")
            try:
                pipeline = queries[i]['pipeline']
            except KeyError:
                abort(400, "Missing pipeline for query " + str(i))
            try:
                operator = queries[i]["operator"]
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
                    abort(400, "Missing metric for field " + str(j) +", pipeline/operator " + pipeline + "/" + operator)
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
                querystring += " LIMIT " + str(limit)
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
            df_current = dataframes(columns[i], influxdata)
            if len(df_current) == 0:
                print("Got empty results for query " + str(i))
                print("Adding empty columns " + str(columns[i]))
                df_current.append(pd.DataFrame(columns=columns[i]))
            for df_single in df_current:
                df.append(df_single)
        hasher = hashlib.sha256()
        hasher.update(request.data)
        return response_from_dfs(df, "metricsquery." + str(hasher.hexdigest()))
