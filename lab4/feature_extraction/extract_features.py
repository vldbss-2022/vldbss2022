import json

from feature_extraction.node_features import class2json, extract_feature_from_node

# plan2seq converts the structure of this plan from a tree to a sequence.
# for example, if the plan is Join1(Scan1, Join2(Scan2, Scan3)), then the final sequence is:
#   [Join1, Scan1, None_Scan1, Join2, Scan2, None_Scan2, Scan3, None_Scan3, None_Join2, None_Join1]
# use None to mark the end of an operator.
def plan2seq(root, alias2table):
    sequence = []
    join_conditions = []
    node, join_condition = extract_feature_from_node(root, alias2table)
    if join_condition is not None:
        join_conditions += join_condition
    sequence.append(node)
    if 'Plans' in root:
        for plan in root['Plans']:
            next_sequence, next_join_conditions = plan2seq(plan, alias2table)
            sequence += next_sequence
            join_conditions += next_join_conditions
    # use None to mark the end of this operator
    sequence.append(None)
    return sequence, join_conditions


# get_alias2table gets all table alias related to this plan.
def get_alias2table(root, alias2table):
    if 'Relation Name' in root and 'Alias' in root:
        alias2table[root['Alias']] = root['Relation Name']
    if 'Plans' in root:
        for child in root['Plans']:
            get_alias2table(child, alias2table)


def feature_extractor(input_path, out_path):
    with open(out_path, 'w') as out:
        with open(input_path, 'r') as f:
            for index, plan in enumerate(f.readlines()):
                print (index)
                if plan != 'null\n':
                    plan = json.loads(plan)[0]['Plan']
                    if plan['Node Type'] == 'Aggregate':
                        plan = plan['Plans'][0]
                    alias2table = {}
                    get_alias2table(plan, alias2table)
                    cost, cardinality = plan['Actual Total Time'], plan['Actual Rows']
                    seq, _ = plan2seq(plan, alias2table)
                    seqs = PlanInSeq(seq, cost, cardinality)
                    out.write(class2json(seqs)+'\n')


class PlanInSeq(object):
    def __init__(self, seq, cost, cardinality):
        self.seq = seq
        self.cost = cost
        self.cardinality = cardinality
