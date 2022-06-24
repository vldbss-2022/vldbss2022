from os import path
import time
import torch
from torch.autograd import Variable
import numpy as np
from util import unnormalize, EncodeContext

from training.representation_model import Representation


def qerror_loss(preds, targets, mini, maxi):
    # TODO:
    qerror = []
    preds = unnormalize(preds, mini, maxi)
    targets = unnormalize(targets, mini, maxi)
    for i in range(len(targets)):
        if (preds[i] > targets[i]).cpu().data.numpy()[0]:
            qerror.append(preds[i]/targets[i])
        else:
            qerror.append(targets[i]/preds[i])
    return torch.mean(torch.cat(qerror)), torch.median(torch.cat(qerror)), torch.max(torch.cat(qerror)), \
           torch.argmax(torch.cat(qerror))


def train_and_test(ctx:EncodeContext, train_start, train_end, validate_start, validate_end, num_epochs, data_dir=path.join('data', 'job')):
    input_dim = ctx.condition_op_dim
    hidden_dim = 128
    hid_dim = 256
    middle_result_dim = 128
    task_num = 2
    model = Representation(input_dim, hidden_dim, hid_dim, middle_result_dim, task_num)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    model.train()
    start = time.time()
    for epoch in range(num_epochs):
        cost_loss_total = 0.
        card_loss_total = 0.
        model.train()
        for batch_idx in range(train_start, train_end):
            print(f'batch_idx:{batch_idx}')
            target_cost, target_cardinality, operatorss, extra_infoss, condition1ss, condition2ss, sampless, condition_maskss, mapping = get_batch_job(batch_idx, directory=data_dir)
            target_cost, target_cardinality, operatorss, extra_infoss, condition1ss, condition2ss, sampless, condition_maskss, mapping = torch.FloatTensor(target_cost), torch.FloatTensor(target_cardinality),torch.FloatTensor(operatorss),torch.FloatTensor(extra_infoss),torch.FloatTensor(condition1ss),torch.FloatTensor(condition2ss), torch.FloatTensor(sampless), torch.FloatTensor(condition_maskss), torch.FloatTensor(mapping)
            operatorss, extra_infoss, condition1ss, condition2ss, condition_maskss = operatorss.squeeze(0), extra_infoss.squeeze(0), condition1ss.squeeze(0), condition2ss.squeeze(0), condition_maskss.squeeze(0).unsqueeze(2)
            sampless = sampless.squeeze(0)
            mapping = mapping.squeeze(0)
            target_cost, target_cardinality, operatorss, extra_infoss, condition1ss, condition2ss = Variable(target_cost), Variable(target_cardinality), Variable(operatorss), Variable(extra_infoss), Variable(condition1ss), Variable(condition2ss)
            sampless = Variable(sampless)
            optimizer.zero_grad()
            estimate_cost,estimate_cardinality = model(operatorss, extra_infoss, condition1ss, condition2ss, sampless, condition_maskss, mapping)
            target_cost = target_cost
            target_cardinality = target_cardinality
            cost_loss,cost_loss_median,cost_loss_max,cost_max_idx = qerror_loss(estimate_cost, target_cost, ctx.cost_label_min, ctx.cost_label_max)
            card_loss,card_loss_median,card_loss_max,card_max_idx = qerror_loss(estimate_cardinality, target_cardinality, ctx.card_label_min, ctx.card_label_max)
            print (card_loss.item(),card_loss_median.item(),card_loss_max.item(),card_max_idx.item())
            loss = cost_loss + card_loss
            cost_loss_total += cost_loss.item()
            card_loss_total += card_loss.item()
            start = time.time()
            loss.backward()
            optimizer.step()
            end = time.time()
            print('batchward time: ',end - start)
        batch_num = train_end - train_start
        print("Epoch {}, training cost loss: {}, training card loss: {}".format(epoch, cost_loss_total/batch_num, card_loss_total/batch_num))

        cost_loss_total = 0.
        card_loss_total = 0.
        for batch_idx in range(validate_start, validate_end):
            print ('batch_idx: ', batch_idx)
            target_cost, target_cardinality, operatorss, extra_infoss, condition1ss, condition2ss, sampless, condition_maskss, mapping = get_batch_job(batch_idx)
            target_cost, target_cardinality, operatorss, extra_infoss, condition1ss, condition2ss, sampless, condition_maskss, mapping = torch.FloatTensor(target_cost), torch.FloatTensor(target_cardinality),torch.FloatTensor(operatorss),torch.FloatTensor(extra_infoss),torch.FloatTensor(condition1ss),torch.FloatTensor(condition2ss), torch.FloatTensor(sampless), torch.FloatTensor(condition_maskss), torch.FloatTensor(mapping)
            operatorss, extra_infoss, condition1ss, condition2ss, condition_maskss = operatorss.squeeze(0), extra_infoss.squeeze(0), condition1ss.squeeze(0), condition2ss.squeeze(0), condition_maskss.squeeze(0).unsqueeze(2)
            sampless = sampless.squeeze(0)
            mapping = mapping.squeeze(0)
            target_cost, target_cardinality, operatorss, extra_infoss, condition1ss, condition2ss = Variable(target_cost), Variable(target_cardinality), Variable(operatorss), Variable(extra_infoss), Variable(condition1ss), Variable(condition2ss)
            sampless = Variable(sampless)
            estimate_cost,estimate_cardinality = model(operatorss, extra_infoss, condition1ss, condition2ss, sampless, condition_maskss, mapping)
            target_cost = target_cost
            target_cardinality = target_cardinality
            cost_loss,cost_loss_median,cost_loss_max,cost_max_idx = qerror_loss(estimate_cost, target_cost, ctx.cost_label_min, ctx.cost_label_max)
            card_loss,card_loss_median,card_loss_max,card_max_idx = qerror_loss(estimate_cardinality, target_cardinality, ctx.card_label_min, ctx.card_label_max)
            print (card_loss.item(),card_loss_median.item(),card_loss_max.item(),card_max_idx.item())
            loss = cost_loss + card_loss
            cost_loss_total += cost_loss.item()
            card_loss_total += card_loss.item()
        batch_num = validate_end - validate_start
        print("Epoch {}, validating cost loss: {}, validating card loss: {}".format(epoch, cost_loss_total/batch_num, card_loss_total/batch_num))
    end = time.time()
    print(end-start)
    return model


def get_batch_job(batch_id, directory=path.join('data', 'job')):
    target_cost_batch = np.load(path.join(directory, f'target_cost_{batch_id}.np.npy'))
    target_cardinality_batch = np.load(path.join(directory, f'target_cardinality_{batch_id}.np.npy'))
    operators_batch = np.load(path.join(directory, f'operators_{batch_id}.np.npy'))
    extra_infos_batch = np.load(path.join(directory, f'extra_infos_{batch_id}.np.npy'))
    condition1s_batch = np.load(path.join(directory, f'condition1s_{batch_id}.np.npy'))
    condition2s_batch = np.load(path.join(directory, f'condition2s_{batch_id}.np.npy'))
    samples_batch = np.load(path.join(directory, f'samples_{batch_id}.np.npy'))
    condition_masks_batch = np.load(path.join(directory, f'condition_masks_{batch_id}.np.npy'))
    mapping_batch = np.load(path.join(directory, f'mapping_{batch_id}.np.npy'))
    return target_cost_batch, target_cardinality_batch, operators_batch, extra_infos_batch, condition1s_batch, \
           condition2s_batch, samples_batch, condition_masks_batch, mapping_batch