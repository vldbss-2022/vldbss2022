import matplotlib.pyplot as plt


def draw_act_est_figure(name, act_rows, est_rows, save_fig=True):
    assert len(act_rows) == len(est_rows)
    plt.figure(figsize=(10, 10))
    plt.axes(xscale='log', yscale='log')
    plt.axline((0, 0), (1, 2), color='grey', linewidth=1)
    plt.axline((0, 0), (2, 1), color='grey', linewidth=1)
    plt.scatter(act_rows, est_rows, s=2)
    plt.xlabel('act_rows')
    plt.ylabel('est_rows')
    plt.title(name)
    if save_fig:
        plt.savefig("./eval/" + name)
    else:
        plt.show()


def cal_p_error(act, est):
    if act < est:
        act, est = est, act
    if est == 0:
        est = 1
    return act/est


def cal_p_error_distribution(act_rows, est_rows):
    assert len(act_rows) == len(est_rows)
    n = len(act_rows)
    p_errors = []
    for i in range(n):
        p_errors.append(cal_p_error(act_rows[i], est_rows[i]))
    p_errors.sort()
    p50 = p_errors[int(n * 0.5)]
    p80 = p_errors[int(n * 0.8)]
    p90 = p_errors[int(n * 0.9)]
    p99 = p_errors[int(n * 0.99)]
    return p50, p80, p90, p99
