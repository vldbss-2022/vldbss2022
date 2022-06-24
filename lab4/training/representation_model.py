from random import sample
import time
import torch
import torch.nn as nn
import torch.nn.functional as F


class Representation(nn.Module):
    def __init__(self, input_dim, hidden_dim, hid_dim, middle_result_dim, task_num):
        super(Representation, self).__init__()
        self.hidden_dim = hidden_dim
        self.lstm1 = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.batch_norm1 = nn.BatchNorm1d(hid_dim)
        # The linear layer that maps from hidden state space to tag space

        self.sample_mlp = nn.Linear(1000, hid_dim)
        self.condition_mlp = nn.Linear(hidden_dim, hid_dim)
        # self.out_mlp1 = nn.Linear(hidden_dim, middle_result_dim)
        # self.hid_mlp1 = nn.Linear(15+108+2*hid_dim, hid_dim)
        # self.out_mlp1 = nn.Linear(hid_dim, middle_result_dim)

        self.lstm2 = nn.LSTM(15 + 108 + 2 * hid_dim, hidden_dim, batch_first=True)
        # self.lstm2_binary = nn.LSTM(15+108+2*hid_dim, hidden_dim, batch_first=True)
        # self.lstm2_binary = nn.LSTM(15+108+2*hid_dim, hidden_dim, batch_first=True)
        self.batch_norm2 = nn.BatchNorm1d(hidden_dim)
        # The linear layer that maps from hidden state space to tag space
        self.hid_mlp2_task1 = nn.Linear(hidden_dim, hid_dim)
        self.hid_mlp2_task2 = nn.Linear(hidden_dim, hid_dim)
        self.batch_norm3 = nn.BatchNorm1d(hid_dim)
        self.hid_mlp3_task1 = nn.Linear(hid_dim, hid_dim)
        self.hid_mlp3_task2 = nn.Linear(hid_dim, hid_dim)
        self.out_mlp2_task1 = nn.Linear(hid_dim, 1)
        self.out_mlp2_task2 = nn.Linear(hid_dim, 1)

        # self.hidden2values2 = nn.Linear(hidden_dim, action_num)

    def init_hidden(self, hidden_dim, batch_size=1):
        # Before we've done anything, we dont have any hidden state.
        # Refer to the Pytorch documentation to see exactly
        # why they have this dimensionality.
        # The axes semantics are (num_layers, minibatch_size, hidden_dim)
        return (torch.zeros(1, batch_size, hidden_dim),
                torch.zeros(1, batch_size, hidden_dim))
    
    # Operators
    def forward(self, operators, extra_infos, condition1s, condition2s, samples, condition_masks, mapping):
        batch_size = 0
        for i in range(operators.size()[1]):
            if operators[0][i].sum(0) != 0:
                batch_size += 1
            else:
                break
        # print(f'batch_size:{batch_size}')

        num_level = condition1s.size()[0]
        num_node_per_level = condition1s.size()[1]
        num_condition_per_node = condition1s.size()[2]
        condition_op_length = condition1s.size()[3]

        # embedding layer
        # YOUR CODE HERE: finish the embedding layer
        # calculate condition_embedding from condition1s and condition2s

        sample_output = F.relu(self.sample_mlp(samples))
        sample_output = sample_output * condition_masks

        # representation layer
        out = torch.cat((operators, extra_infos, condition_embedding, sample_output), 2)
        # print (out.size())
        # torch.Size([14, 133, 635])
        # out = out * node_masks
        start = time.time()
        hidden = self.init_hidden(self.hidden_dim, num_node_per_level)
        last_level = out[num_level - 1].view(num_node_per_level, 1, -1)
        _, (hid, cid) = self.lstm2(last_level, hidden)
        mapping = mapping.long()
        for idx in reversed(range(0, num_level - 1)):
            # YOUR CODE HERE: select indexes of left/right children
            # calculate mapp_left/mapp_right here.

            pad = torch.zeros_like(hid)[:, 0].unsqueeze(1)
            next_hid = torch.cat((pad, hid), 1)
            pad = torch.zeros_like(cid)[:, 0].unsqueeze(1)
            next_cid = torch.cat((pad, cid), 1)
            hid_left = torch.index_select(next_hid, 1, mapp_left)   # hidden states of left children 
            cid_left = torch.index_select(next_cid, 1, mapp_left)   # cell states of left children
            hid_right = torch.index_select(next_hid, 1, mapp_right) # hidden states of right children
            cid_right = torch.index_select(next_cid, 1, mapp_right) # cell states of right children
            # YOUR CODE HERE: calculate hid and cid of this level

        output = hid[0]
        # print (output.size())
        # torch.Size([133, 128])
        end = time.time()
        print(f'Forest Evaluate Running Time:{end - start}')
        last_output = output[0:batch_size]
        out = self.batch_norm2(last_output)

        # estimation layer
        out_task1 = F.relu(self.hid_mlp2_task1(out))
        out_task1 = self.batch_norm3(out_task1)
        out_task1 = F.relu(self.hid_mlp3_task1(out_task1))
        out_task1 = self.out_mlp2_task1(out_task1)
        out_task1 = F.sigmoid(out_task1)

        out_task2 = F.relu(self.hid_mlp2_task2(out))
        out_task2 = self.batch_norm3(out_task2)
        out_task2 = F.relu(self.hid_mlp3_task2(out_task2))
        out_task2 = self.out_mlp2_task2(out_task2)
        out_task2 = F.sigmoid(out_task2)
        # print(f'out:{out.size()}')
        # batch_size * task_num
        return out_task1, out_task2