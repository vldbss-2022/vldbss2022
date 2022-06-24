import torch
import numpy as np

def normalize_label(labels, mini, maxi):
    labels_norm = (np.log(labels) - mini) / (maxi - mini)
    labels_norm = np.minimum(labels_norm, np.ones_like(labels_norm))
    labels_norm = np.maximum(labels_norm, np.zeros_like(labels_norm))
    return labels_norm


def unnormalize(vecs, mini, maxi):
    return torch.exp(vecs * (maxi - mini) + mini)

class EncodeContext:
    def __init__(self, data, word_vectors, min_max_values, tables_id, columns_id, indexes_id, physical_ops_id,
                 compare_ops_id, bool_ops_id, plan_node_max_num, condition_max_num, cost_label_min, cost_label_max,
                 card_label_min, card_label_max):
        self.data = data
        self.word_vectors = word_vectors
        self.min_max_values = min_max_values
        self.tables_id = tables_id
        self.columns_id = columns_id
        self.indexes_id = indexes_id
        self.physical_ops_id = physical_ops_id
        self.compare_ops_id = compare_ops_id
        self.bool_ops_id = bool_ops_id
        self.condition_op_dim = len(bool_ops_id) + len(compare_ops_id) + len(columns_id) + 1000
        self.plan_node_max_num = plan_node_max_num
        self.condition_max_num = condition_max_num
        self.cost_label_min = cost_label_min
        self.cost_label_max = cost_label_max
        self.card_label_min = card_label_min
        self.card_label_max = card_label_max
