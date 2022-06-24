from plan_encoding.encode_string import get_str_representation
from util import EncodeContext
import numpy as np
import re


# encode_condition encodes these conditions.
def encode_conditions(ctx: EncodeContext, conditions, relation_name, index_name, condition_max_num):
    if len(conditions) == 0:
        vecs = [[0] * ctx.condition_op_dim]
    else:
        vecs = [encode_condition_op(ctx, condition_op, relation_name, index_name) for condition_op in conditions]
    num_pad = condition_max_num - len(vecs)
    result = np.pad(vecs, ((0, num_pad), (0, 0)), 'constant')
    return result


# encode_condition_op encodes the specified condition.
def encode_condition_op(ctx: EncodeContext, condition_op, relation_name, index_name):
    # bool_operator + left_value + compare_operator + right_value
    if condition_op is None:
        vec = [0] * ctx.condition_op_dim
    elif condition_op['op_type'] == 'Bool':
        idx = ctx.bool_ops_id[condition_op['operator']]
        vec = [0] * len(ctx.bool_ops_id)
        vec[idx-1] = 1
    else:
        operator = condition_op['operator']
        left_value = condition_op['left_value']
        if re.match(r'.+\..+', left_value) is None:
            if relation_name is None:
                relation_name = index_name.split(left_value)[1].strip('_')
            left_value = relation_name + '.' + left_value
        else:
            relation_name = left_value.split('.')[0]
        left_value_idx = ctx.columns_id[left_value]
        left_value_vec = [0] * len(ctx.columns_id)
        left_value_vec[left_value_idx-1] = 1
        right_value = condition_op['right_value']
        column_name = left_value.split('.')[1]
        if re.match(r'^[a-z][a-zA-Z0-9_]*\.[a-z][a-zA-Z0-9_]*$', right_value) is not None and right_value.split('.')[0] in ctx.data:
            # col op col
            operator_idx = ctx.compare_ops_id[operator]
            operator_vec = [0] * len(ctx.compare_ops_id)
            operator_vec[operator_idx-1] = 1
            right_value_idx = ctx.columns_id[right_value]
            right_value_vec = [0]
            left_value_vec[right_value_idx-1] = 1
        elif ctx.data[relation_name].dtypes[column_name] == 'int64' or ctx.data[relation_name].dtypes[column_name] == 'float64':
            # col op num_val
            right_value = float(right_value)
            value_max = ctx.min_max_values[relation_name][column_name]['max']
            value_min = ctx.min_max_values[relation_name][column_name]['min']
            # YOUR CODE HERE: encode this condition "col op num_val"
            # calculate right_value_vec and operator_vec here

        elif re.match(r'^__LIKE__', right_value) is not None:
            # col like str_var
            operator_idx = ctx.compare_ops_id['~~']
            operator_vec = [0] * len(ctx.compare_ops_id)
            operator_vec[operator_idx-1] = 1
            right_value = right_value.strip('\'')[8:]
            right_value_vec = get_str_representation(ctx.word_vectors, right_value, left_value).tolist()
        elif re.match(r'^__NOTLIKE__', right_value) is not None:
            # col not-like str_var
            operator_idx = ctx.compare_ops_id['!~~']
            operator_vec = [0] * len(ctx.compare_ops_id)
            operator_vec[operator_idx-1] = 1
            right_value = right_value.strip('\'')[11:]
            right_value_vec = get_str_representation(ctx.word_vectors, right_value, left_value).tolist()
        elif re.match(r'^__NOTEQUAL__', right_value) is not None:
            # col not-eq str_var
            operator_idx = ctx.compare_ops_id['!=']
            operator_vec = [0] * len(ctx.compare_ops_id)
            operator_vec[operator_idx-1] = 1
            right_value = right_value.strip('\'')[12:]
            right_value_vec = get_str_representation(ctx.word_vectors, right_value, left_value).tolist()
        elif re.match(r'^__ANY__', right_value) is not None:
            operator_idx = ctx.compare_ops_id['=']
            operator_vec = [0] * len(ctx.compare_ops_id)
            operator_vec[operator_idx-1] = 1
            right_value = right_value.strip('\'')[7:].strip('{}')
            right_value_vec = []
            count = 0
            for v in right_value.split(','):
                v = v.strip('"').strip('\'')
                if len(v) > 0:
                    count += 1
                    vec = get_str_representation(ctx.word_vectors, v, left_value).tolist()
                    if len(right_value_vec) == 0:
                        right_value_vec = [0 for _ in vec]
                    for idx, vv in enumerate(vec):
                        right_value_vec[idx] += vv
            for idx in range(len(right_value_vec)):
                right_value_vec[idx] /= len(right_value.split(','))
        elif right_value == 'None':
            operator_idx = ctx.compare_ops_id['!Null']
            operator_vec = [0] * len(ctx.compare_ops_id)
            operator_vec[operator_idx-1] = 1
            if operator == 'IS':
                right_value_vec = [1]
            elif operator == '!=':
                right_value_vec = [0]
            else:
                print (operator)
                raise
        else:
            # print(f'left_value:{left_value}, operator:{operator}, right_value:{right_value}')
            operator_idx = ctx.compare_ops_id[operator]
            operator_vec = [0] * len(ctx.compare_ops_id)
            operator_vec[operator_idx-1] = 1
            right_value_vec = get_str_representation(ctx.word_vectors, right_value, left_value).tolist()
        vec = [0] * len(ctx.bool_ops_id)
        vec = vec + left_value_vec + operator_vec + right_value_vec
    num_pad = ctx.condition_op_dim - len(vec)
    result = np.pad(vec, (0, num_pad), 'constant')
    # print(f'condition_op:{result}')
    return result