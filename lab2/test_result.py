import pytest
import json

class Test_simple():
    def test_cost(self):
        json_file = 'lab1/eval/results.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
        act = data.get('avi')
        tidb = data.get('tidb')
        learning = data.get('learning')
        calibration = data.get('calibration')

        assert(len(act) == len(tidb))
        assert(len(act) == len(learning))
        assert(len(act) == len(calibration))

        for v in learning:
            assert(v>0)
        for v in calibration:
            assert(v>0)


if __name__ == '__main__':
    pytest.main(['test_result.py'])
