import json
import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    user="root",
    port="4000",
    passwd=""
)
cursor = db.cursor()

query_file = './data/query.json'
with open(query_file, 'r') as f:
    qs = json.load(f)

cursor.execute("set @@tidb_external_cardinality_estimator_address='http://127.0.0.1:8888/cardinality'")
cursor.execute("set @@tidb_external_cost_estimator_address='http://127.0.0.1:8888/cost'")

results = []
for q in qs:
    cursor.execute("explain format='verbose' " + q)
    raw_plan = cursor.fetchall()
    plan = []
    for x in raw_plan:
        plan.append('\t'.join(x))
    cursor.execute("show warnings")
    warnings = cursor.fetchall()
    ws = []
    for w in warnings:
        ws.append('\t'.join([w[0], w[2]]))
    result = {
        "query": q,
        "plan": plan,
        "warnings": ws
    }
    results.append(result)
    print(result)

with open('./eval/results.json', 'w') as outfile:
    json.dump(results, outfile)
