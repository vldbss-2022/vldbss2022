import torch
import torch.nn as nn

from plan import Operator

operators = ["Projection", "Selection", "Sort", "HashAgg", "HashJoin", "TableScan", "IndexScan", "TableRowIDScan",
             "TableReader", "IndexReader", "IndexLookUp"]


class PlanFeatureCollector:
    def __init__(self):
        # YOUR CODE HERE: you can add features extracted from plan.
        # self.op_count = 0
        # self.count_per_op = [0] * len(operators)
        # self.rows_per_op = [0] * len(operators)
        pass

    def add_operator(self, op: Operator):
        # YOUR CODE HERE: update features by op
        pass

    def walk_operator_tree(self, op: Operator):
        self.add_operator(op)
        for child in op.children:
            self.walk_operator_tree(child)
        # YOUR CODE HERE: concat features as a vector
        # return [self.op_count] + self.count_per_op + self.rows_per_op


class PlanDataset(torch.utils.data.Dataset):
    def __init__(self, plans):
        super().__init__()
        self.data = []
        for plan in plans:
            collector = PlanFeatureCollector()
            features = torch.Tensor(collector.walk_operator_tree(plan.root))
            exec_time = torch.Tensor([plan.exec_time_in_ms()])
            self.data.append((features, exec_time))

    def __getitem__(self, index):
        return self.data[index]

    def __len__(self):
        return len(self.data)


# Define your model for cost estimation
class YourModel(nn.Module):
    def __init__(self):
        super().__init__()
        # YOUR CODE HERE

    def forward(self, x):
        # YOUR CODE HERE
        pass

    def init_weights(self):
        # YOUR CODE HERE
        pass


def estimate_learning(train_plans, test_plans):
    train_dataset = PlanDataset(train_plans)
    train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=10, shuffle=False, num_workers=1)

    model = YourModel()
    model.init_weights()

    def loss_fn(est_time, act_time):
        # YOUR CODE HERE: define loss function
        pass

    # YOUR CODE HERE: complete training loop
    num_epoch = 20
    for epoch in range(num_epoch):
        print(f"epoch {epoch} start")
        for i, data in enumerate(train_loader):
            pass

    train_est_times, train_act_times = [], []
    for i, data in enumerate(train_loader):
        # YOUR CODE HERE: evaluate on train data
        pass

    test_dataset = PlanDataset(test_plans)
    test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=10, shuffle=True, num_workers=1)

    test_est_times, test_act_times = [], []
    for i, data in enumerate(test_loader):
        # YOUR CODE HERE: evaluate on test data
        pass

    return train_est_times, train_act_times, test_est_times, test_act_times
