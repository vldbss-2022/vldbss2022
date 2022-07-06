import pytest
import json

class Test_simple():
    def test_cost(self):
        json_file = 'lab1/eval/result.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
        act_cost = data.get('avi_cost')
        est_cost = data.get('est_cost')
        act_cardinality = data.get('act_cardinality')
        est_cardinality = data.get('est_cardinality')
        if len(act_cost) != len(est_cost):
            assert False
        if [i for i in est_cost if i <= 0]:
            assert False
        if abs(act_cardinality-est_cardinality)< 0.2 and abs(act_cost-est_cost) < 0.3:
            assert True


if __name__ == '__main__':
    pytest.main(['test_result.py'])