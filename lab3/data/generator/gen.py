import json

xx = '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/summer-school/lab2/data/test_plans.json'
with open(xx, 'r') as f:
    cases = json.load(f)


def remove_hint(q):
    for (b, e) in [("/*", "*/"), ("USE INDEX", ")")]:
        pb = q.find(b)
        if pb == -1:
            continue
        pe = q.find(e, pb+len(b))
        if pe == -1:
            continue
        q = q[:pb] + q[pe+len(e):]
    return q

qs = []
for c in cases:
    q = c['query']
    qs.append(remove_hint(q))

with open("/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/summer-school/lab3/data/query.json", "w") as outfile:
    outfile.write(json.dumps(qs, indent=4))

