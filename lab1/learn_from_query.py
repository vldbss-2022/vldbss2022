import evaluation_utils as eval_utils
import matplotlib.pyplot as plt
import numpy as np
import range_query as rq
import json
import torch
import torch.nn as nn
import statistics as stats
import xgboost as xgb

from statistics import AVIEstimator, ExpBackoffEstimator, MinSelEstimator

class Args:
    def __init__(self, **kwargs):
        self.bs = 32
        self.epochs = 1000
        self.lr = 0.0001
        self.hid_units = '128_64_32'
        # self.bins = 200
        # self.train_num = 10000

        # overwrite parameters from user
        self.__dict__.update(kwargs)


def min_max_normalize(v, min_v, max_v):
    # The function may be useful when dealing with lower/upper bounds of columns.
    assert max_v > min_v
    return (v-min_v)/(max_v-min_v)


def extract_features_from_query(range_query, table_stats, considered_cols):
    # feat:     [c1_begin, c1_end, c2_begin, c2_end, ... cn_begin, cn_end, AVI_sel, EBO_sel, Min_sel]
    #           <-                   range features                    ->, <-     est features     ->
    feature = []
    # YOUR CODE HERE: extract features from query
    for col in considered_cols:
        min_val = table_stats.columns[col].min_val()
        max_val = table_stats.columns[col].max_val()
        left_val, right_val = range_query.column_range(col, min_val, max_val)
        feature.append(min_max_normalize(left_val, min_val, max_val))
        feature.append(min_max_normalize(right_val, min_val, max_val))
    feature.append(stats.AVIEstimator.estimate(range_query, table_stats))
    feature.append(stats.ExpBackoffEstimator.estimate(range_query, table_stats))
    feature.append(stats.MinSelEstimator.estimate(range_query, table_stats))
    return feature


def preprocess_queries(queris, table_stats, columns):
    """
    preprocess_queries turn queries into features and labels, which are used for regression model.
    """
    features, labels = [], []
    for item in queris:
        query, act_rows = item['query'], item['act_rows']
        feature, label = None, None
        # YOUR CODE HERE: transform (query, act_rows) to (feature, label)
        # Some functions like rq.ParsedRangeQuery.parse_range_query and extract_features_from_query may be helpful.
        range_query = rq.ParsedRangeQuery.parse_range_query(query)
        feature = extract_features_from_query(range_query, table_stats, columns)
        label = int(act_rows)
        
        features.append(feature)
        labels.append(label)
    return features, labels


class QueryDataset(torch.utils.data.Dataset):
    def __init__(self, queries, table_stats, columns):
        super().__init__()
        self.query_data = list(zip(*preprocess_queries(queries, table_stats, columns)))

    def __getitem__(self, index):
        # return self.query_data[index]
        data = torch.FloatTensor(self.query_data[index][0])
        list = []
        list.append(self.query_data[index][1])
        label = torch.Tensor(list)
        return data, label

    def __len__(self):
        return len(self.query_data)
    
class LWNNLayer(nn.Module):
    def __init__(self, input_len, output_len):
        super().__init__()
        self.layer = nn.Sequential(
            nn.Linear(input_len, output_len),
            nn.ReLU(inplace=True),
        )

    def forward(self, X):
        return self.layer(X)


class LWNNModel(nn.Module):
    def __init__(self, input_len, hid_units):
        super().__init__()
        self.hid_units = hid_units

        self.hid_layers = nn.Sequential()
        for l, output_len in enumerate([int(u) for u in hid_units.split('_')]):
            self.hid_layers.add_module('layer_{}'.format(l), LWNNLayer(input_len, output_len))
            input_len = output_len

        self.final = nn.Linear(input_len, 1)

    def forward(self, X):
        mid_out = self.hid_layers(X)
        pred = self.final(mid_out)

        return pred

    def name(self):
        return f"lwnn_hid{self.hid_units}"


def est_mlp(train_data, test_data, table_stats, columns):
    """
    est_mlp uses MLP to produce estimated rows for train_data and test_data
    """
    args = Args()

    train_dataset = QueryDataset(train_data, table_stats, columns)
    train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=args.bs, shuffle=False, num_workers=1)
    train_est_rows, train_act_rows = [], []
    # YOUR CODE HERE: train procedure
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    feat_num = len(columns) * 2 + 3
    lw_nn_model = LWNNModel(feat_num, args.hid_units)
    lw_nn_model.to(device)
    optimizer = torch.optim.Adam(lw_nn_model.parameters(), lr=args.lr)
    
    def loss_fn(predict_rows, actual_rows):
        est_rows = torch.abs(predict_rows)
        return torch.mean(torch.square(torch.log2(torch.abs(predict_rows)) - torch.log2(actual_rows)))
    
    mse_loss = nn.MSELoss(reduction='none')
    print(f"Train on train set...")
    for epoch in range(args.epochs):
        train_loss = 0.0
        lw_nn_model.train()
        for _, data in enumerate(train_loader):
            inputs, labels = data
            inputs = inputs.to(device).float()
            labels = labels.to(device).float()

            optimizer.zero_grad()
            preds = lw_nn_model(inputs)

            if epoch == args.epochs - 1:
                train_act_rows += labels.squeeze(1).tolist()
                train_est_rows += preds.squeeze(1).tolist()

            loss = mse_loss(preds, labels)
            loss.mean().backward()
            optimizer.step()
            train_loss += torch.sum(loss).item()
        print(f"Epoch {epoch + 1}, loss: {train_loss/len(train_loader)}")

    test_dataset = QueryDataset(test_data, table_stats, columns)
    test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=args.bs, shuffle=False, num_workers=1)
    test_est_rows, test_act_rows = [], []
    # YOUR CODE HERE: test procedure
    print(f"Test on test set...")

    test_loss = 0.0
    lw_nn_model.eval()
    for _, data in enumerate(test_loader):
        inputs, labels = data
        inputs = inputs.to(device).float()
        labels = labels.to(device).float()

        with torch.no_grad():
            preds = lw_nn_model(inputs)

            test_act_rows += labels.squeeze(1).tolist()
            test_est_rows += preds.squeeze(1).tolist()

            loss = mse_loss(preds, labels)
            test_loss += torch.sum(loss).item()

    print(f'Test loss is {test_loss/len(test_loader):.4f}')
    
    # print(len(train_act_rows))
    # print(len(train_est_rows))
    # print(len(test_act_rows))
    # print(len(test_act_rows))

    return train_est_rows, train_act_rows, test_est_rows, test_act_rows


def est_xgb(train_data, test_data, table_stats, columns):
    """
    est_xgb uses xgboost to produce estimated rows for train_data and test_data
    """
    print("estimate row counts by xgboost")
    train_x, train_y = preprocess_queries(train_data, table_stats, columns)
    train_est_rows, train_act_rows = [], []
    # YOUR CODE HERE: train procedure

    test_x, test_y = preprocess_queries(test_data, table_stats, columns)
    test_est_rows, test_act_rows = [], []
    # YOUR CODE HERE: test procedure

    return train_est_rows, train_act_rows, test_est_rows, test_act_rows


def eval_model(model, train_data, test_data, table_stats, columns):
    if model == 'mlp':
        est_fn = est_mlp
    else:
        est_fn = est_xgb

    train_est_rows, train_act_rows, test_est_rows, test_act_rows = est_fn(train_data, test_data, table_stats, columns)

    name = f'{model}_train_{len(train_data)}'
    eval_utils.draw_act_est_figure(name, train_act_rows, train_est_rows)
    p50, p80, p90, p99 = eval_utils.cal_p_error_distribution(train_act_rows, train_est_rows)
    print(f'{name}, p50:{p50}, p80:{p80}, p90:{p90}, p99:{p99}')

    name = f'{model}_test_{len(test_data)}'
    eval_utils.draw_act_est_figure(name, test_act_rows, test_est_rows)
    p50, p80, p90, p99 = eval_utils.cal_p_error_distribution(test_act_rows, test_est_rows)
    print(f'{name}, p50:{p50}, p80:{p80}, p90:{p90}, p99:{p99}')


if __name__ == '__main__':
    stats_json_file = './data/title_stats.json'
    train_json_file = './data/query_train_20000.json'
    test_json_file = './data/query_test_5000.json'
    columns = ['kind_id', 'production_year', 'imdb_id', 'episode_of_id', 'season_nr', 'episode_nr']
    table_stats = stats.TableStats.load_from_json_file(stats_json_file, columns)
    with open(train_json_file, 'r') as f:
        train_data = json.load(f)
    with open(test_json_file, 'r') as f:
        test_data = json.load(f)

    eval_model('mlp', train_data, test_data, table_stats, columns)
    eval_model('xgb', train_data, test_data, table_stats, columns)
