import matplotlib.pyplot as plt


def draw_act_est_figure(name, est_costs, act_times, save_fig=True):
    assert len(est_costs) == len(act_times)
    plt.figure(figsize=(10, 10))
    # plt.axes(xscale='log', yscale='log')
    # plt.axline((0, 0), (1, 2), color='grey', linewidth=1)
    # plt.axline((0, 0), (2, 1), color='grey', linewidth=1)
    plt.scatter(est_costs, act_times, s=2)
    plt.xlabel('est_cost')
    plt.ylabel('act_time')
    plt.title(name)
    if save_fig:
        plt.savefig("./eval/" + name)
    else:
        plt.show()