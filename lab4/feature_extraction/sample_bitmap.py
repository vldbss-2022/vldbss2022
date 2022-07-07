import json
import re
import pandas as pd


def add_sample_bitmap(input_path, output_path, data, sample, sample_num):
    with open(input_path, 'r') as ff:
        with open(output_path, 'w') as f:
            for count, plan in enumerate(ff.readlines()):
                print (count)
                parsed_plan = json.loads(plan)
                nodes_with_sample = []
                for node in parsed_plan['seq']:
                    bitmap_filter = []
                    bitmap_index = []
                    bitmap_other = []
                    if node != None and 'condition' in node:
                        bitmap_other = get_preds_bitmap(node['condition'], data, sample, sample_num)
                    if node != None and 'condition_filter' in node:
                        bitmap_filter = get_preds_bitmap(node['condition_filter'], data, sample, sample_num)
                    if node != None and 'condition_index' in node:
                        bitmap_index = get_preds_bitmap(node['condition_index'], data, sample, sample_num)
                    if len(bitmap_filter) > 0 or len(bitmap_index) > 0 or len(bitmap_other) > 0:
                        # YOUR CODE HERE: merge these bitmaps into a single bitmap.
                        # You can use the function bitand here.
                        node['bitmap'] = ''.join([str(x) for x in bitmap])
                    nodes_with_sample.append(node)
                parsed_plan['seq'] = nodes_with_sample
                f.write(json.dumps(parsed_plan))
                f.write('\n')
                

def get_preds_bitmap(preds, data, sample, sample_num):
    if len(preds) == 0:
        return []
    root = TreeNode(preds[0], None)
    if len(preds) > 1:
        recover_pred_tree(preds[1:], root)
    return get_bitmap_from_predtree(root, data, sample, sample_num)


def get_bitmap_from_predtree(root, data, sample, sample_num):
    predicate = root.get_item()
    if predicate is not None and predicate['op_type'] == 'Compare':
        table_name = predicate['left_value'].split('.')[0]
        column = predicate['left_value'].split('.')[1]
        vec = []
        pattern = re.compile(r'^[a-z_]+\.[a-z][a-z0-9_]*$')
        result = pattern.match(predicate['right_value'])
        if result is None:
            dtype = data[table_name].dtypes[column]
            for value in sample[table_name][column].tolist():
                if isSelected(value, predicate, dtype):
                    vec.append(1)
                else:
                    vec.append(0)
            for i in range(len(vec), sample_num):
                vec.append(0)
        elif not predicate['right_value'].split('.')[0] in data:
            dtype = data[table_name].dtypes[column]
            for value in sample[table_name][column].tolist():
                if isSelected(value, predicate, dtype):
                    vec.append(1)
                else:
                    vec.append(0)
            for i in range(len(vec), sample_num):
                vec.append(0)
        return vec
    elif predicate is not None and predicate['op_type'] == 'Bool':
        bitmap = []
        if predicate['operator'] == 'AND':
            # YOUR CODE HERE: get the bitmap for this AND operator.
            # You can use the function bitand here.
        elif predicate['operator'] == 'OR':
            # YOUR CODE HERE: get the bitmap for this OR operator.
            # You can use the function bitor here.
        else:
            print(predicate['operator'])
            raise
        return bitmap
    else:
        return []


def isSelected(row_value, predicate, dtype):
    if dtype == 'int64':
        row_value = int(row_value)
        value = int(predicate['right_value'])
        op = predicate['operator']
        if op == '=':
            if row_value != value:
                return False
        elif op == '!=':
            if row_value == value:
                return False
        elif op == '<':
            if row_value >= value:
                return False
        elif op == '>':
            if row_value <= value:
                return False
        elif op == '<=':
            if row_value > value:
                return False
        elif op == '>=':
            if row_value < value:
                return False
        else:
            print(op)
            raise
    elif dtype == 'float64':
        row_value = float(row_value)
        value = float(predicate['right_value'])
        op = predicate['operator']
        if op == '=':
            if row_value != value:
                return False
        elif op == '!=':
            if row_value == value:
                return False
        elif op == '<':
            if row_value >= value:
                return False
        elif op == '>':
            if row_value <= value:
                return False
        elif op == '<=':
            if row_value > value:
                return False
        elif op == '>=':
            if row_value < value:
                return False
        else:
            print(op)
            raise
    elif dtype == 'object':
        value = predicate['right_value']
        op = predicate['operator']
        if pd.isnull(row_value):
            row_value = ''
        else:
            row_value = str(row_value)
        if op == '=':
            if value.startswith('__LIKE__'):
                v = value[8:]
                pattern = r'^'
                for idx, token in enumerate(v.split('%')):
                    if len(token) == 0:
                        pattern += r'.*'
                    else:
                        pattern += re.escape(token)
                        if idx < len(v.split('%')) - 1:
                            pattern += r'.*'
                pattern += r'$'
                if re.match(pattern, row_value) == None:
                    return False
            elif value.startswith('__NOTLIKE__'):
                v = value[11:]
                pattern = r'^'
                for idx, token in enumerate(v.split('%')):
                    if len(token) == 0:
                        pattern += r'.*'
                    else:
                        pattern += re.escape(token)
                        if idx < len(v.split('%')) - 1:
                            pattern += r'.*'
                pattern += r'$'
                if re.match(pattern, row_value) != None:
                    return False
            elif value.startswith('__NOTEQUAL__'):
                pattern = value[12:]
                if row_value == pattern:
                    return False
            elif value.startswith('__ANY__'):
                pattern = value.strip('__ANY__')
                pattern = pattern.strip('{}')
                for token in pattern.split(','):
                    token = token.strip('"').strip('\'')
                    if row_value == token:
                        return True
                return False
            elif value == 'None':
                if len(row_value) > 0:
                    return False
            else:
                if row_value != value:
                    return False
        elif op == 'IS':
            if value == 'None':
                if len(row_value) > 0:
                    return False
            else:
                print(value)
                raise
        elif op == '!=':
            if value == 'None':
                if len(row_value) == 0:
                    return False
            else:
                if row_value == value:
                    return False
        elif op == '<':
            if row_value >= value:
                return False
        elif op == '>':
            if row_value <= value:
                return False
        elif op == '<=':
            if row_value > value:
                return False
        elif op == '>=':
            if row_value < value:
                return False
        else:
            print(op)
            raise
    else:
        print(dtype)
        raise
    return True


class TreeNode(object):
    def __init__(self, current_vec, parent):
        self.item = current_vec
        self.parent = parent
        self.children = []

    def get_parent(self):
        return self.parent

    def get_item(self):
        return self.item

    def get_children(self):
        return self.children

    def add_child(self, child):
        self.children.append(child)


def recover_pred_tree(vecs, parent):
    if len(vecs) == 0:
        return vecs
    if vecs[0] is None:
        return vecs[1:]
    node = TreeNode(vecs[0], parent)
    while True:
        vecs = recover_pred_tree(vecs[1:], node)
        parent.add_child(node)
        if len(vecs) == 0:
            return vecs
        if vecs[0] is None:
            return vecs[1:]
        node = TreeNode(vecs[0], parent)


def bitand(bit1, bit2):
    if len(bit1) > 0 and len(bit2) > 0:
        result = []
        for i in range(len(bit1)):
            result.append(min(bit1[i], bit2[i]))
        return result
    elif len(bit1) > 0:
        return bit1
    elif len(bit2) > 0:
        return bit2
    else:
        return []


def bitor(bit1, bit2):
    if len(bit1) > 0 and len(bit2) > 0:
        result = []
        for i in range(len(bit1)):
            result.append(max(bit1[i], bit2[i]))
        return result
    elif len(bit1) > 0:
        return bit1
    elif len(bit2) > 0:
        return bit2
    else:
        return []


def prepare_samples(data, sample_num, tables):
    sample = dict()
    for table_name in tables:
        sample[table_name] = data[table_name].sample(n=min(sample_num, len(data[table_name])))
    return sample