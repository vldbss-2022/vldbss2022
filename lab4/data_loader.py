from gensim.models import KeyedVectors
import json
import math
from os import path
import pandas as pd


# load_imdb_dataset loads imdb datasets from CSV files in the specified directory.
def load_imdb_dataset(dir_path):
    data = dict()
    data["aka_name"] = pd.read_csv(dir_path + '/aka_name.csv', header=None)
    data["aka_title"] = pd.read_csv(dir_path + '/aka_title.csv', header=None)
    data["cast_info"] = pd.read_csv(dir_path + '/cast_info.csv', header=None)
    data["char_name"] = pd.read_csv(dir_path + '/char_name.csv', header=None)
    data["company_name"] = pd.read_csv(dir_path + '/company_name.csv', header=None)
    data["company_type"] = pd.read_csv(dir_path + '/company_type.csv', header=None)
    data["comp_cast_type"] = pd.read_csv(dir_path + '/comp_cast_type.csv', header=None)
    data["complete_cast"] = pd.read_csv(dir_path + '/complete_cast.csv', header=None)
    data["info_type"] = pd.read_csv(dir_path + '/info_type.csv', header=None)
    data["keyword"] = pd.read_csv(dir_path + '/keyword.csv', header=None)
    data["kind_type"] = pd.read_csv(dir_path + '/kind_type.csv', header=None)
    data["link_type"] = pd.read_csv(dir_path + '/link_type.csv', header=None)
    data["movie_companies"] = pd.read_csv(dir_path + '/movie_companies.csv', header=None)
    data["movie_info"] = pd.read_csv(dir_path + '/movie_info.csv', header=None)
    data["movie_info_idx"] = pd.read_csv(dir_path + '/movie_info_idx.csv', header=None)
    data["movie_keyword"] = pd.read_csv(dir_path + '/movie_keyword.csv', header=None)
    data["movie_link"] = pd.read_csv(dir_path + '/movie_link.csv', header=None)
    data["name"] = pd.read_csv(dir_path + '/name.csv', header=None)
    data["person_info"] = pd.read_csv(dir_path + '/person_info.csv', header=None)
    data["role_type"] = pd.read_csv(dir_path + '/role_type.csv', header=None)
    data["title"] = pd.read_csv(dir_path + '/title.csv', header=None)

    aka_name_column = {
        'id': 0,
        'person_id': 1,
        'name': 2,
        'imdb_index': 3,
        'name_pcode_cf': 4,
        'name_pcode_nf': 5,
        'surname_pcode': 6,
        'md5sum': 7
    }

    aka_title_column = {
        'id': 0,
        'movie_id': 1,
        'title': 2,
        'imdb_index': 3,
        'kind_id': 4,
        'production_year': 5,
        'phonetic_code': 6,
        'episode_of_id': 7,
        'season_nr': 8,
        'episode_nr': 9,
        'note': 10,
        'md5sum': 11
    }

    cast_info_column = {
        'id': 0,
        'person_id': 1,
        'movie_id': 2,
        'person_role_id': 3,
        'note': 4,
        'nr_order': 5,
        'role_id': 6
    }

    char_name_column = {
        'id': 0,
        'name': 1,
        'imdb_index': 2,
        'imdb_id': 3,
        'name_pcode_nf': 4,
        'surname_pcode': 5,
        'md5sum': 6
    }

    comp_cast_type_column = {
        'id': 0,
        'kind': 1
    }

    company_name_column = {
        'id': 0,
        'name': 1,
        'country_code': 2,
        'imdb_id': 3,
        'name_pcode_nf': 4,
        'name_pcode_sf': 5,
        'md5sum': 6
    }

    company_type_column = {
        'id': 0,
        'kind': 1
    }

    complete_cast_column = {
        'id': 0,
        'movie_id': 1,
        'subject_id': 2,
        'status_id': 3
    }

    info_type_column = {
        'id': 0,
        'info': 1
    }

    keyword_column = {
        'id': 0,
        'keyword': 1,
        'phonetic_code': 2
    }

    kind_type_column = {
        'id': 0,
        'kind': 1
    }

    link_type_column = {
        'id': 0,
        'link': 1
    }

    movie_companies_column = {
        'id': 0,
        'movie_id': 1,
        'company_id': 2,
        'company_type_id': 3,
        'note': 4
    }

    movie_info_idx_column = {
        'id': 0,
        'movie_id': 1,
        'info_type_id': 2,
        'info': 3,
        'note': 4
    }

    movie_keyword_column = {
        'id': 0,
        'movie_id': 1,
        'keyword_id': 2
    }

    movie_link_column = {
        'id': 0,
        'movie_id': 1,
        'linked_movie_id': 2,
        'link_type_id': 3
    }

    name_column = {
        'id': 0,
        'name': 1,
        'imdb_index': 2,
        'imdb_id': 3,
        'gender': 4,
        'name_pcode_cf': 5,
        'name_pcode_nf': 6,
        'surname_pcode': 7,
        'md5sum': 8
    }

    role_type_column = {
        'id': 0,
        'role': 1
    }

    title_column = {
        'id': 0,
        'title': 1,
        'imdb_index': 2,
        'kind_id': 3,
        'production_year': 4,
        'imdb_id': 5,
        'phonetic_code': 6,
        'episode_of_id': 7,
        'season_nr': 8,
        'episode_nr': 9,
        'series_years': 10,
        'md5sum': 11
    }

    movie_info_column = {
        'id': 0,
        'movie_id': 1,
        'info_type_id': 2,
        'info': 3,
        'note': 4
    }

    person_info_column = {
        'id': 0,
        'person_id': 1,
        'info_type_id': 2,
        'info': 3,
        'note': 4
    }
    data["aka_name"].columns = aka_name_column
    data["aka_title"].columns = aka_title_column
    data["cast_info"].columns = cast_info_column
    data["char_name"].columns = char_name_column
    data["company_name"].columns = company_name_column
    data["company_type"].columns = company_type_column
    data["comp_cast_type"].columns = comp_cast_type_column
    data["complete_cast"].columns = complete_cast_column
    data["info_type"].columns = info_type_column
    data["keyword"].columns = keyword_column
    data["kind_type"].columns = kind_type_column
    data["link_type"].columns = link_type_column
    data["movie_companies"].columns = movie_companies_column
    data["movie_info"].columns = movie_info_column
    data["movie_info_idx"].columns = movie_info_idx_column
    data["movie_keyword"].columns = movie_keyword_column
    data["movie_link"].columns = movie_link_column
    data["name"].columns = name_column
    data["person_info"].columns = person_info_column
    data["role_type"].columns = role_type_column
    data["title"].columns = title_column
    return data


# prepare_imdb_dataset prepares unique IDs for all tables, columns, indexes, operators and operations.
def prepare_imdb_dataset_for_extraction(database):
    column2pos = dict()

    tables = ['aka_name', 'aka_title', 'cast_info', 'char_name', 'company_name', 'company_type', 
              'comp_cast_type', 'complete_cast', 'info_type', 'keyword', 'kind_type', 'link_type', 
              'movie_companies', 'movie_info', 'movie_info_idx', 'movie_keyword', 'movie_link', 
              'name', 'person_info', 'role_type', 'title']

    for table_name in tables:
        column2pos[table_name] = database[table_name].columns

    indexes = ['aka_name_pkey', 'aka_title_pkey', 'cast_info_pkey', 'char_name_pkey',
               'comp_cast_type_pkey', 'company_name_pkey', 'company_type_pkey', 'complete_cast_pkey',
               'info_type_pkey', 'keyword_pkey', 'kind_type_pkey', 'link_type_pkey', 'movie_companies_pkey',
               'movie_info_idx_pkey', 'movie_keyword_pkey', 'movie_link_pkey', 'name_pkey', 'role_type_pkey',
               'title_pkey', 'movie_info_pkey', 'person_info_pkey', 'company_id_movie_companies',
               'company_type_id_movie_companies', 'info_type_id_movie_info_idx', 'info_type_id_movie_info',
               'info_type_id_person_info', 'keyword_id_movie_keyword', 'kind_id_aka_title', 'kind_id_title',
               'linked_movie_id_movie_link', 'link_type_id_movie_link', 'movie_id_aka_title', 'movie_id_cast_info',
               'movie_id_complete_cast', 'movie_id_movie_ companies', 'movie_id_movie_info_idx',
               'movie_id_movie_keyword', 'movie_id_movie_link', 'movie_id_movie_info', 'person_id_aka_name',
               'person_id_cast_info', 'person_id_person_info', 'person_role_id_cast_info', 'role_id_cast_info']
    indexes_id = dict()
    for idx, index in enumerate(indexes):
        indexes_id[index] = idx + 1
    physic_ops_id = {'Materialize':1, 'Sort':2, 'Hash':3, 'Merge Join':4, 'Bitmap Index Scan':5,
                     'Index Only Scan':6, 'BitmapAnd':7, 'Nested Loop':8, 'Aggregate':9, 'Result':10,
                     'Hash Join':11, 'Seq Scan':12, 'Bitmap Heap Scan':13, 'Index Scan':14, 'BitmapOr':15}
    strategy_id = {'Plain':1}
    compare_ops_id = {'=':1, '>':2, '<':3, '!=':4, '~~':5, '!~~':6, '!Null': 7, '>=':8, '<=':9}
    bool_ops_id = {'AND':1,'OR':2}
    tables_id = {}
    columns_id = {}
    table_id = 1
    column_id = 1
    for table_name in tables:
        tables_id[table_name] = table_id
        table_id += 1
        for column in column2pos[table_name]:
            columns_id[table_name+'.'+column] = column_id
            column_id += 1
    return column2pos, indexes_id, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, tables


def prepare_imdb_dataset_for_encoding(prefix_path='data'):
    tables = ['aka_name', 'aka_title', 'cast_info', 'char_name', 'company_name', 'company_type', 'comp_cast_type',
              'complete_cast', 'info_type', 'keyword', 'kind_type', 'link_type', 'movie_companies', 'movie_info',
              'movie_info_idx', 'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type', 'title']

    aka_name_columns = ['id', 'person_id', 'name', 'imdb_index', 'name_pcode_cf', 'name_pcode_nf', 'surname_pcode',
                        'md5sum']
    aka_title_columns = ['id', 'movie_id', 'title', 'imdb_index', 'kind_id', 'production_year', 'phonetic_code',
                         'episode_of_id', 'season_nr', 'episode_nr', 'note', 'md5sum']
    cast_info_columns = ['id', 'person_id', 'movie_id', 'person_role_id', 'note', 'nr_order', 'role_id']
    char_name_columns = ['id', 'name', 'imdb_index', 'imdb_id', 'name_pcode_nf', 'surname_pcode', 'md5sum']
    comp_cast_type_columns = ['id', 'kind']
    company_name_columns = ['id', 'name', 'country_code', 'imdb_id', 'name_pcode_nf', 'name_pcode_sf', 'md5sum']
    company_type_columns = ['id', 'kind']
    complete_cast_columns = ['id', 'movie_id', 'subject_id', 'status_id']
    info_type_columns = ['id', 'info']
    keyword_columns = ['id', 'keyword', 'phonetic_code']
    kind_type_columns = ['id', 'kind']
    link_type_columns = ['id', 'link']
    movie_companies_columns = ['id', 'movie_id', 'company_id', 'company_type_id', 'note']
    movie_info_idx_columns = ['id', 'movie_id', 'info_type_id', 'info', 'note']
    movie_keyword_columns = ['id', 'movie_id', 'keyword_id']
    movie_link_columns = ['id', 'movie_id', 'linked_movie_id', 'link_type_id']
    name_columns = ['id', 'name', 'imdb_index', 'imdb_id', 'gender', 'name_pcode_cf', 'name_pcode_nf', 'surname_pcode',
                    'md5sum']
    role_type_columns = ['id', 'role']
    title_columns = ['id', 'title', 'imdb_index', 'kind_id', 'production_year', 'imdb_id', 'phonetic_code',
                     'episode_of_id', 'season_nr', 'episode_nr', 'series_years', 'md5sum']
    movie_info_columns = ['id', 'movie_id', 'info_type_id', 'info', 'note']
    person_info_columns = ['id', 'person_id', 'info_type_id', 'info', 'note']

    table_columns = {'aka_name': aka_name_columns, 'aka_title': aka_title_columns, 'cast_info': cast_info_columns,
                     'char_name': char_name_columns, 'company_name': company_name_columns,
                     'company_type': company_type_columns, 'comp_cast_type': comp_cast_type_columns,
                     'complete_cast': complete_cast_columns, 'info_type': info_type_columns, 'keyword': keyword_columns,
                     'kind_type': kind_type_columns, 'link_type': link_type_columns,
                     'movie_companies': movie_companies_columns, 'movie_info': movie_info_columns,
                     'movie_info_idx': movie_info_idx_columns, 'movie_keyword': movie_keyword_columns,
                     'movie_link': movie_link_columns, 'name': name_columns, 'person_info': person_info_columns,
                     'role_type': role_type_columns, 'title': title_columns}

    data = {table_name: pd.read_csv(path.join(prefix_path, 'imdb_data_csv', f'{table_name}.csv'), header=None)
            for table_name in tables}

    for table, columns in table_columns.items():
        data[table].columns = columns

    tables_id, columns_id = {}, {}
    table_id, column_id = 1, 1
    for table_name in tables:
        tables_id[table_name] = table_id
        table_id += 1
        for column_name in table_columns[table_name]:
            columns_id[table_name + '.' + column_name] = column_id
            column_id += 1

    indexes = ['aka_name_pkey', 'aka_title_pkey', 'cast_info_pkey', 'char_name_pkey',
               'comp_cast_type_pkey', 'company_name_pkey', 'company_type_pkey', 'complete_cast_pkey',
               'info_type_pkey', 'keyword_pkey', 'kind_type_pkey', 'link_type_pkey', 'movie_companies_pkey',
               'movie_info_idx_pkey', 'movie_keyword_pkey', 'movie_link_pkey', 'name_pkey', 'role_type_pkey',
               'title_pkey', 'movie_info_pkey', 'person_info_pkey', 'company_id_movie_companies',
               'company_type_id_movie_companies', 'info_type_id_movie_info_idx', 'info_type_id_movie_info',
               'info_type_id_person_info', 'keyword_id_movie_keyword', 'kind_id_aka_title', 'kind_id_title',
               'linked_movie_id_movie_link', 'link_type_id_movie_link', 'movie_id_aka_title', 'movie_id_cast_info',
               'movie_id_complete_cast', 'movie_id_movie_ companies', 'movie_id_movie_info_idx',
               'movie_id_movie_keyword', 'movie_id_movie_link', 'movie_id_movie_info', 'person_id_aka_name',
               'person_id_cast_info', 'person_id_person_info', 'person_role_id_cast_info', 'role_id_cast_info']

    indexes_id = {index: i + 1 for i, index in enumerate(indexes)}

    physical_ops_id = {'Materialize': 1, 'Sort': 2, 'Hash': 3, 'Merge Join': 4, 'Bitmap Index Scan': 5,
                       'Index Only Scan': 6, 'BitmapAnd': 7, 'Nested Loop': 8, 'Aggregate': 9, 'Result': 10,
                       'Hash Join': 11, 'Seq Scan': 12, 'Bitmap Heap Scan': 13, 'Index Scan': 14, 'BitmapOr': 15}
    compare_ops_id = {'=': 1, '>': 2, '<': 3, '!=': 4, '~~': 5, '!~~': 6, '!Null': 7, '>=': 8, '<=': 9}
    bool_ops_id = {'AND': 1, 'OR': 2}

    return data, tables_id, columns_id, indexes_id, physical_ops_id, compare_ops_id, bool_ops_id


def load_dictionary(file_path):
    word_vectors = KeyedVectors.load(file_path, mmap='r')
    return word_vectors


def load_numeric_min_max(file_path):
    with open(file_path, 'r') as f:
        min_max_values = json.loads(f.read())
    return min_max_values


def load_plans_and_obtain_bounds(file_path):
    plans = []
    plan_node_max_num = 0
    condition_max_num = 0
    cost_label_min, cost_label_max = 9999999999.0, 0.0
    card_label_min, card_label_max = 9999999999.0, 0.0
    with open(file_path, 'r') as f:
        for plan in f.readlines():
            plan = json.loads(plan)
            plans.append(plan)
            cost = plan['cost']
            cardinality = plan['cardinality']
            cost_label_max = max(cost, cost_label_max)
            cost_label_min = min(cost, cost_label_min)
            card_label_max = max(cardinality, card_label_max)
            card_label_min = min(cardinality, card_label_min)
            sequence = plan['seq']
            plan_node_max_num = max(len(sequence), plan_node_max_num)
            for node in sequence:
                if node is None:
                    continue
                if 'condition_filter' in node:
                    condition_max_num = max(len(node['condition_filter']), condition_max_num)
                if 'condition_index' in node:
                    condition_max_num = max(len(node['condition_index']), condition_max_num)
    cost_label_min, cost_label_max = math.log(cost_label_min), math.log(cost_label_max)
    card_label_min, card_label_max = math.log(card_label_min), math.log(card_label_max)
    print(f'plan_node_max_num={plan_node_max_num}, condition_max_num={condition_max_num}')
    print(f'cost_label_min={cost_label_min}, cost_label_max={cost_label_max}')
    print(f'card_label_min={card_label_min}, card_label_max={card_label_max}')
    return plans, plan_node_max_num, condition_max_num, cost_label_min, cost_label_max, card_label_min, card_label_max


