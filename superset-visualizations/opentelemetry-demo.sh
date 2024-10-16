#!/bin/bash
#    Copyright 2024 Mishmash IO UK Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

export FLASK_APP=superset

# Just init everything as a completely new Superset installation:
superset db upgrade && \
superset fab create-admin \
    --username admin \
    --firstname OpenTelemetry \
    --lastname Demo \
    --email os@mishmash.io \
    --password admin && \
superset init && \
superset import-directory /app/opentelemetry-import-resources && \
exec superset run -p 8088 --with-threads -h 0.0.0.0
