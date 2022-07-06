import pytest
import json

class Test_simple():
    def test_cost(self):
        json_file = 'lab1/eval/results.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
        act = data.get('avi')
        tidb = data.get('est')
        learning = data.get('learning')
        calibration = data.get('calibration')
        if len(act) != len(tidb):
            assert False
        if [i for i in learning if i <= 0]:
            assert False



if __name__ == '__main__':
    pytest.main(['test_result.py'])
