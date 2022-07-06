import pytest
import json

class Test_simple():
    def test_ml(self):
        json_file = 'lab1/eval/results.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
        act = data.get('act')
        avi = data.get('avi')
        est = data.get('est')
        mlp = data.get('mlp')
        xgb = data.get('xgb')
        if len(act) != len(est):
            assert False
        if [i for i in est if i <= 0]:
            assert False
        if len(mlp) > 0  or len(xgb) >0 :
            assert True

    def test_spn(self):
        json_file = 'lab1/eval/result.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
        if [x[1] for x in data.items() if 'spn_' in x[0]]:
            assert True

if __name__ == '__main__':
    pytest.main(['test_result.py'])