import json

from feature_extraction.predicate_features import *


# change_alias2table changes alias to the real table name
def change_alias2table(column, alias2table):
    relation_name = column.split('.')[0]
    column_name = column.split('.')[1]
    if relation_name in alias2table:
        return alias2table[relation_name] + '.' + column_name
    else:
        return column


# extract_feature_from_node extracts features from this node.
def extract_feature_from_node(node, alias2table):
    relation_name, index_name = None, None
    if 'Relation Name' in node:
        relation_name = node['Relation Name']
    if 'Index Name' in node:
        index_name = node['Index Name']

    if node['Node Type'] == 'Sort':
        # YOUR CODE HERE: extract features from this Sort node.

    elif node['Node Type'] == 'Hash Join':
        return Join('Hash Join', pre2seq(node["Hash Cond"], alias2table, relation_name, index_name)), None
    elif node['Node Type'] == 'Hash':
        return Hash(), None
    elif node['Node Type'] == 'Merge Join':
        return Join('Merge Join', pre2seq(node["Merge Cond"], alias2table, relation_name, index_name)), None
    elif node['Node Type'] == 'Nested Loop':
        if 'Join Filter' in node:
            condition = pre2seq(node['Join Filter'], alias2table, relation_name, index_name)
        else:
            condition = []
        return Join('Nested Loop', condition), None
    elif node['Node Type'] == 'Aggregate':
        keys = []
        if 'Group Key' in node:
            keys = [change_alias2table(key, alias2table) for key in node['Group Key']]
        return Aggregate(node['Strategy'], keys), None
    elif node['Node Type'] == 'Seq Scan':
        # YOUR CODE HERE: extract features from this Seq Scan node.

    elif node['Node Type'] == 'Index Scan':
        condition_seq_filter, condition_seq_index = [], []
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relation_name, index_name)
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relation_name, index_name)
        relation_name, index_name = node["Relation Name"], node['Index Name']
        if len(condition_seq_index) == 1 and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Index Scan', condition_seq_filter, condition_seq_index, relation_name,
                        index_name), condition_seq_index
        else:
            return Scan('Index Scan', condition_seq_filter, condition_seq_index, relation_name, index_name), None
    else:
        raise Exception('Unsupported Node Type: ' + node['Node Type'])


class Aggregate(object):
    def __init__(self, strategy, keys):
        self.node_type = 'Aggregate'
        self.strategy = strategy
        self.group_keys = keys

    def __str__(self):
        return 'Aggregate ON: ' + ','.join(self.group_keys)


class Sort(object):
    def __init__(self, sort_keys):
        self.sort_keys = sort_keys
        self.node_type = 'Sort'

    def __str__(self):
        return 'Sort by: ' + ','.join(self.sort_keys)


class Join(object):
    def __init__(self, node_type, condition_seq):
        self.node_type = node_type
        self.condition = condition_seq

    def __str__(self):
        return self.node_type + ' ON ' + ','.join([str(i) for i in self.condition])


class Scan(object):
    def __init__(self, node_type, condition_seq_filter, condition_seq_index, relation_name, index_name):
        self.node_type = node_type
        self.condition_filter = condition_seq_filter
        self.condition_index = condition_seq_index
        self.relation_name = relation_name
        self.index_name = index_name

    def __str__(self):
        return self.node_type + ' ON ' + ','.join([str(i) for i in self.condition_filter]) + '; ' + ','.join(
            [str(i) for i in self.condition_index])


class Hash(object):
    def __init__(self):
        self.node_type = 'Hash'

    def __str__(self):
        return 'Hash'


def class2json(instance):
    if instance is None:
        return json.dumps({})
    else:
        return json.dumps(todict(instance))


def todict(obj, classkey=None):
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = todict(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return todict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [todict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, todict(value, classkey))
                     for key, value in obj.__dict__.items()
                     if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj