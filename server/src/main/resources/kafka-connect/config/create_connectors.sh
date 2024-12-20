#!/bin/bash

# curl -X DELETE http://connect:8083/connectors/jdbc-postgressql-source-capacity
# curl -X DELETE http://connect:8083/connectors/jdbc-postgressql-source-suppliers
# curl -X DELETE http://connect:8083/connectors/jdbc-sink-results

# Debug curl -X GET http://localhost:8083/connectors/jdbc-sink-results/status

curl -X POST -H "Accept: application/json" -H "Content-type: application/json" http://connect:8083/connectors -d @sink.json
curl -X POST -H "Accept: application/json" -H "Content-type: application/json" http://connect:8083/connectors -d @source1.json
curl -X POST -H "Accept: application/json" -H "Content-type: application/json" http://connect:8083/connectors -d @source2.json