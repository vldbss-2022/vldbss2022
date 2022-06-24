import numpy as np


def determine_prefix(column):
    relation_name = column.split('.')[0]
    column_name = column.split('.')[1]
    if relation_name == 'aka_title':
        if column_name == 'title':
            return 'title_'
        else:
            print(column)
            raise
    elif relation_name == 'char_name':
        if column_name == 'name':
            return 'name_'
        elif column_name == 'name_pcode_nf':
            return 'nf_'
        elif column_name == 'surname_pcode':
            return 'surname_'
        else:
            print(column)
            raise
    elif relation_name == 'movie_info_idx':
        if column_name == 'info':
            return 'info_'
        else:
            print(column)
            raise
    elif relation_name == 'title':
        if column_name == 'title':
            return 'title_'
        else:
            print(column)
            raise
    elif relation_name == 'role_type':
        if column_name == 'role':
            return 'role_'
        else:
            print(column)
            raise
    elif relation_name == 'movie_companies':
        if column_name == 'note':
            return 'note_'
        else:
            print(column)
            raise
    elif relation_name == 'info_type':
        if column_name == 'info':
            return 'info_'
        else:
            print(column)
            raise
    elif relation_name == 'company_type':
        if column_name == 'kind':
            return ''
        else:
            print(column)
            raise
    elif relation_name == 'company_name':
        if column_name == 'name':
            return 'cn_name_'
        elif column_name == 'country_code':
            return 'country_'
        else:
            print(column)
            raise
    elif relation_name == 'keyword':
        if column_name == 'keyword':
            return 'keyword_'
        else:
            print(column)
            raise

    elif relation_name == 'movie_info':
        if column_name == 'info':
            return ''
        elif column_name == 'note':
            return 'note_'
        else:
            print(column)
            raise
    elif relation_name == 'name':
        if column_name == 'gender':
            return 'gender_'
        elif column_name == 'name':
            return 'name_'
        elif column_name == 'name_pcode_cf':
            return 'cf_'
        elif column_name == 'name_pcode_nf':
            return 'nf_'
        elif column_name == 'surname_pcode':
            return 'surname_'
        else:
            print(column)
            raise
    elif relation_name == 'aka_name':
        if column_name == 'name':
            return 'name_'
        elif column_name == 'name_pcode_cf':
            return 'cf_'
        elif column_name == 'name_pcode_nf':
            return 'nf_'
        elif column_name == 'surname_pcode':
            return 'surname_'
        else:
            print(column)
            raise
    elif relation_name == 'link_type':
        if column_name == 'link':
            return 'link_'
        else:
            print(column)
            raise
    elif relation_name == 'person_info':
        if column_name == 'note':
            return 'note_'
        else:
            print(column)
            raise
    elif relation_name == 'cast_info':
        if column_name == 'note':
            return 'note_'
        else:
            print(column)
            raise
    elif relation_name == 'comp_cast_type':
        if column_name == 'kind':
            return 'kind_'
        else:
            print(column)
            raise
    elif relation_name == 'kind_type':
        if column_name == 'kind':
            return 'kind_'
        else:
            print(column)
            raise
    else:
        print(column)
        raise


def get_representation(word_vectors, value):
    if value in word_vectors:
        embedded_result = np.array(list(word_vectors[value]))
    else:
        embedded_result = np.array([0.0] * 500)
    hash_result = np.array([0.0] * 500)
    for t in value:
        hash_result[hash(t) % 500] = 1.0
    return np.concatenate((embedded_result, hash_result), 0)


def get_str_representation(word_vectors, value, column):
    vec = np.array([])
    count = 0
    prefix = determine_prefix(column)
    for v in value.split('%'):
        if len(v) > 0:
            if len(vec) == 0:
                vec = get_representation(word_vectors, prefix+v)
                count = 1
            else:
                new_vec = get_representation(word_vectors, prefix+v)
                vec = vec + new_vec
                count += 1
    if count > 0:
        vec = vec / float(count)
    return vec
