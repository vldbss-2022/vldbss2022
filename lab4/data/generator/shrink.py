import json
import pandas as pd
import random


def shrink_imdb_data(input_dir, output_dir):
    imdb_csvs = ['aka_name.csv', 'aka_title.csv', 'cast_info.csv', 'char_name.csv', 'company_name.csv', 
    'company_type.csv', 'comp_cast_type.csv', 'complete_cast.csv', 'info_type.csv', 'keyword.csv', 
    'kind_type.csv', 'link_type.csv', 'movie_companies.csv', 'movie_info.csv', 'movie_info_idx.csv',
    'movie_keyword.csv', 'movie_link.csv', 'name.csv', 'person_info.csv', 'role_type.csv', 'title.csv']
    for imdb_csv in imdb_csvs:
        input_path = input_dir + '/' + imdb_csv
        output_path = output_dir + '/' + imdb_csv

        df = pd.read_csv(input_path, header=None, low_memory=False)
        ratio = 0.05
        while True:
            sdf = df.sample(frac=ratio)
            if sdf.shape[0] > 50 or ratio == 1:
                sdf.to_csv(output_path, header=False, index=False)
                break
            ratio = ratio * 2
            if ratio > 1:
                ratio = 1

        print("shrink %s to %s" % (imdb_csv, output_path))


valid_node_types = ["Sort", "Hash", "Hash Join", "Merge Join", "Nested Loop", "Aggregate", "Seq Scan", "Index Scan"]

def is_valid_plan(node):
    if node['Node Type'] not in valid_node_types:
        return False
    if 'Plans' in node:
        for subplan in node['Plans']:
            if not is_valid_plan(subplan):
                return False
    return True


def has_join_or_agg(node):
    if node['Node Type'] in ["Hash Join", "Merge Join", "Aggregate"]:
        return True
    if 'Plans' in node:
        for subplan in node['Plans']:
            if has_join_or_agg(subplan):
                return True
    return False

def shrink_plans(input, output, n=100):
    cnt = 0
    with open(output, 'w') as out:
        with open(input, 'r') as f:
            for index, plan in enumerate(f.readlines()):
                print(index)
                if plan != 'null\n':
                    raw = plan
                    plan = json.loads(plan)[0]['Plan']
                    if plan['Node Type'] == 'Aggregate':
                        plan = plan['Plans'][0]
                    if is_valid_plan(plan) and has_join_or_agg(plan):
                        out.write(raw)
                        cnt += 1
                        if cnt >= n:
                            return

# shrink_imdb_data('/Users/zhangyuanjia/Workspace/go/src/github.com/greatji/Learning-based-cost-estimator/data/test/imdb_data_csv',
#     '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/summer-school/lab4/data/imdb_data_csv')
shrink_plans('/Users/zhangyuanjia/Workspace/go/src/github.com/greatji/Learning-based-cost-estimator/data/test/plans.json', 
    '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/summer-school/lab4/data/plans.json', 1600)