from plan_encoding.encode_condition import encode_conditions
from plan_encoding.tree import parse_plan_tree

import numpy as np
from os import path
import torch
from util import normalize_label, EncodeContext


def make_batch(arr, batch_size):
    return [arr[i:i + batch_size] for i in range(0, len(arr), batch_size)]


# encode_and_save_job_plans encodes these JOB plans and saves results into the out_dir.
def encode_and_save_job_plans(ctx: EncodeContext, plans, batch_size=64, out_dir=path.join('data', 'job')):
    print("total plans: %s, batch size: %s" % (len(plans), batch_size))
    for batch_id, plans_batch in enumerate(make_batch(plans, batch_size)):
        print(f'saving data for batch {batch_id}, plan_num={len(plans_batch)}')
        target_cost_batch, target_cardinality_batch, operators_batch, extra_infos_batch, condition1s_batch, \
        condition2s_batch, samples_batch, condition_masks_batch, mapping_batch = encode_job_plan_batch(ctx, plans_batch)
        np.save(path.join(out_dir, f'target_cost_{batch_id}.np'), target_cost_batch.numpy())
        np.save(path.join(out_dir, f'target_cardinality_{batch_id}.np'), target_cardinality_batch.numpy())
        np.save(path.join(out_dir, f'operators_{batch_id}.np'), operators_batch.numpy())
        np.save(path.join(out_dir, f'extra_infos_{batch_id}.np'), extra_infos_batch.numpy())
        np.save(path.join(out_dir, f'condition1s_{batch_id}.np'), condition1s_batch.numpy())
        np.save(path.join(out_dir, f'condition2s_{batch_id}.np'), condition2s_batch.numpy())
        np.save(path.join(out_dir, f'samples_{batch_id}.np'), samples_batch.numpy())
        np.save(path.join(out_dir, f'condition_masks_{batch_id}.np'), condition_masks_batch.numpy())
        np.save(path.join(out_dir, f'mapping_{batch_id}.np'), mapping_batch.numpy())
        print(f'data saved for batch {batch_id}')


# encode_job_plan_batch encodes a batch of JOB plans.
def encode_job_plan_batch(ctx: EncodeContext, plans):
    target_cost_batch = []
    target_card_batch = []
    operators_batch = []
    extra_infos_batch = []
    condition1s_batch = []
    condition2s_batch = []
    samples_batch = []
    condition_masks_batch = []
    mapping_batch = []

    for plan in plans:
        target_cost = plan['cost']
        target_cardinality = plan['cardinality']
        target_cost_batch.append(target_cost)
        target_card_batch.append(target_cardinality)
        plan = plan['seq']
        operators, extra_infos, condition1s, condition2s, samples, condition_masks, mapping \
            = encode_job_plan(ctx, plan, ctx.condition_max_num)

        operators_batch = merge_plans_level(operators_batch, operators)
        extra_infos_batch = merge_plans_level(extra_infos_batch, extra_infos)
        condition1s_batch = merge_plans_level(condition1s_batch, condition1s)
        condition2s_batch = merge_plans_level(condition2s_batch, condition2s)
        samples_batch = merge_plans_level(samples_batch, samples)
        condition_masks_batch = merge_plans_level(condition_masks_batch, condition_masks)
        mapping_batch = merge_plans_level(mapping_batch, mapping, True)

    max_nodes = 0
    for o in operators_batch:
        if len(o) > max_nodes:
            max_nodes = len(o)
    print("max nodes: ", max_nodes, len(condition2s_batch))
    operators_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0)), 'constant') for v in operators_batch])
    extra_infos_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0)), 'constant') for v in extra_infos_batch])
    condition1s_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0), (0, 0)), 'constant') for v in condition1s_batch])
    condition2s_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0), (0, 0)), 'constant') for v in condition2s_batch])
    samples_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0)), 'constant') for v in samples_batch])
    condition_masks_batch = np.array([np.pad(v, (0, max_nodes - len(v)), 'constant') for v in condition_masks_batch])
    mapping_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0)), 'constant') for v in mapping_batch])

    print('operators_batch: ', operators_batch.shape)
    
    target_cost_batch = torch.FloatTensor(target_cost_batch)
    target_card_batch = torch.FloatTensor(target_card_batch)
    operators_batch = torch.FloatTensor([operators_batch])
    extra_infos_batch = torch.FloatTensor([extra_infos_batch])
    condition1s_batch = torch.FloatTensor([condition1s_batch])
    condition2s_batch = torch.FloatTensor([condition2s_batch])
    samples_batch = torch.FloatTensor([samples_batch])
    condition_masks_batch = torch.FloatTensor([condition_masks_batch])
    mapping_batch = torch.FloatTensor([mapping_batch])

    target_cost_batch = normalize_label(target_cost_batch, ctx.cost_label_min, ctx.cost_label_max)
    target_card_batch = normalize_label(target_card_batch, ctx.card_label_min, ctx.card_label_max)

    return target_cost_batch, target_card_batch, operators_batch, extra_infos_batch, condition1s_batch, \
           condition2s_batch, samples_batch, condition_masks_batch, mapping_batch


# encode_job_plan encodes a specified JOB plan.
def encode_job_plan(ctx: EncodeContext, plan, condition_max_num):
    operators, extra_infos, condition1s, condition2s, samples, condition_masks = [], [], [], [], [], []
    mapping = []
    nodes_by_level = parse_plan_tree(plan)

    for level in nodes_by_level:
        operators.append([])
        extra_infos.append([])
        condition1s.append([])
        condition2s.append([])
        samples.append([])
        condition_masks.append([])
        mapping.append([])
        for node in level:
            operator, extra_info, condition1, condition2, sample, condition_mask = encode_job_plan_node(ctx, node.item, condition_max_num)
            operators[-1].append(operator)
            extra_infos[-1].append(extra_info)
            condition1s[-1].append(condition1)
            condition2s[-1].append(condition2)
            samples[-1].append(sample)
            condition_masks[-1].append(condition_mask)
            if len(node.children) == 2:
                mapping[-1].append([n.idx for n in node.children])
            elif len(node.children) == 1:
                mapping[-1].append([node.children[0].idx, 0])
            else:
                mapping[-1].append([0, 0])
    return operators, extra_infos, condition1s, condition2s, samples, condition_masks, mapping


# encode_job_plan_node encodes a JOB plan node.
def encode_job_plan_node(ctx: EncodeContext, node, condition_max_num):
    # operator + first_condition + second_condition + relation
    extra_info_num = max(len(ctx.columns_id), len(ctx.tables_id), len(ctx.indexes_id))
    extra_info_vec = np.array([0] * extra_info_num)                                     # meta data: indicate which column, table or index is referred
    operator_vec = np.array([0] * len(ctx.physical_ops_id))                             # indicate the node type
    condition1_vec = np.array([[0] * ctx.condition_op_dim] * condition_max_num)
    condition2_vec = np.array([[0] * ctx.condition_op_dim] * condition_max_num)
    sample_vec = np.array([1] * 1000)
    has_bitmap = 0

    if node is not None:
        node_type = node['node_type']
        operator_idx = ctx.physical_ops_id[node_type]
        operator_vec[operator_idx - 1] = 1
        if node_type == 'Sort':
            for key in node['sort_keys']:
                extra_info_inx = ctx.columns_id[key]
                extra_info_vec[extra_info_inx - 1] = 1
        elif node_type == 'Hash Join' or node_type == 'Merge Join' or node_type == 'Nested Loop':
            condition1_vec = encode_conditions(ctx, node['condition'], None, None, condition_max_num)
        elif node_type == 'Aggregate':
            for key in node['group_keys']:
                extra_info_inx = ctx.columns_id[key]
                extra_info_vec[extra_info_inx - 1] = 1
        elif node_type == 'Seq Scan' or node_type == 'Index Scan':
            relation_name = node['relation_name']
            index_name = node['index_name']
            if relation_name is not None:
                extra_info_inx = ctx.tables_id[relation_name]
            else:
                extra_info_inx = ctx.indexes_id[index_name]
            extra_info_vec[extra_info_inx - 1] = 1
            # YOUR CODE HERE: encode condition_filter to condition1_vec and condition_index to condition2_vec and bitmap
            # to sample_vec. You can use the function encode_conditions and encode_sample here.


    return operator_vec, extra_info_vec, condition1_vec, condition2_vec, sample_vec, has_bitmap


def encode_sample(sample):
    return np.array([int(i) for i in sample])


def bitand(sample1, sample2):
    return np.minimum(sample1, sample2)


def merge_plans_level(level1, level2, is_mapping=False):
    for idx, level in enumerate(level2):
        if idx >= len(level1):
            level1.append([])
        if is_mapping:
            if idx < len(level1) - 1:
                base = len(level1[idx + 1])
                for i in range(len(level)):
                    if level[i][0] > 0:
                        level[i][0] += base
                    if level[i][1] > 0:
                        level[i][1] += base
        level1[idx] += level
    return level1

