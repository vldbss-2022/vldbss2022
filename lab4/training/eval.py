from os import path
import time
import torch
import json
from torch.autograd import Variable
import numpy as np
from util import unnormalize, EncodeContext

from training.train_and_test import qerror_loss, get_batch_job
import matplotlib.pyplot as plt


def model_eval(ctx:EncodeContext, model, eval_start, eval_end):
    cost_loss_total = 0.
    card_loss_total = 0.
    est_costs, target_costs = torch.tensor([]), torch.tensor([])
    est_cards, target_cards = torch.tensor([]), torch.tensor([])
    for batch_idx in range(eval_start, eval_end):
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

        est_costs = torch.cat([est_costs, unnormalize(estimate_cost, ctx.cost_label_min, ctx.cost_label_max)])
        target_costs = torch.cat([target_costs, unnormalize(target_cost, ctx.cost_label_min, ctx.cost_label_max)])
        est_cards = torch.cat([est_cards, unnormalize(estimate_cardinality, ctx.card_label_min, ctx.card_label_max)])
        target_cards = torch.cat([target_cards, unnormalize(target_cardinality, ctx.card_label_min, ctx.card_label_max)])

    batch_num = eval_end - eval_start
    print("evaluating cost loss: {}, evaluating card loss: {}".format(cost_loss_total/batch_num, card_loss_total/batch_num))

    est_costs = est_costs.squeeze()
    target_costs = target_costs.squeeze()
    est_cards = est_cards.squeeze()
    target_cards = target_cards.squeeze()

    draw_act_est_figure("cost", est_costs, target_costs, True)
    draw_act_est_figure("cardinality", est_cards, target_cards, True)

    with open('./eval/results.json', 'w') as outfile:
        json.dump({
            "act_cost": target_costs.tolist(),
            "est_cost": est_costs.tolist(),
            "act_cardinality": target_cards.tolist(),
            "est_cardinality": est_cards.tolist()
        }, outfile)    

    return model


def draw_act_est_figure(name, est_vals, target_vals, save_fig=True):
    assert len(est_vals) == len(target_vals)
    plt.figure(figsize=(10, 10))
    plt.axes(xscale='log', yscale='log')
    plt.scatter(est_vals.detach().numpy(), target_vals.detach().numpy(), s=2)
    plt.xlabel('est_' + name)
    plt.ylabel('act_' + name)
    plt.title(name)
    if save_fig:
        plt.savefig("./doc/" + name)
    else:
        plt.show()