import pytest
import json

class Test_simple():
    def test_lab4(self):
        json_file = 'lab4/eval/results.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
        act_cost = data.get('act_cost')
        est_cost = data.get('est_cost')
        act_cardinality = data.get('act_cardinality')
        est_cardinality = data.get('est_cardinality')
        
        assert(len(act_cost) > 0)
        assert(len(act_cost) == len(est_cost))
        assert(len(act_cost) == len(act_cardinality))
        assert(len(act_cost) == len(est_cardinality))
        assert(sum(est_cardinality) > 0)
        assert(sum(est_cost) > 0)

if __name__ == '__main__':
    pytest.main(['test_result.py'])