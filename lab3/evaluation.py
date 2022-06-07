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

# YOUR CODE HERE: set your estimator addresses
cursor.execute("set @@tidb_external_cardinality_estimator_address='http://127.0.0.1:8888/cardinality'")
cursor.execute("set @@tidb_external_cost_estimator_address='http://127.0.0.1:8888/cost'")

for q in qs:
    cursor.execute("explain format='verbose' " + q)
    raw_plan = cursor.fetchall()
    print(q)
    for x in raw_plan:
        print("  " + "\t".join(list(x)))
    print()
    cursor.execute("show warnings")
    warnings = cursor.fetchall()
    assert (len(warnings) == 0)
