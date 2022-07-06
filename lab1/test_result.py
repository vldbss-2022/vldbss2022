import pytest
import json

class Test_simple():
    def test_card(self):
        json_file = 'lab1/eval/results.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
        act = data.get('act')
        avi = data.get('avi')
        ebo = data.get('ebo')
        min_sel = data.get('min_sel')
        mlp = data.get('mlp')
        xgb = data.get('xgb')
        spn1000 = data.get('spn_sample_1000')
        spn10000 = data.get('spn_sample_10000')
        spn20000 = data.get('spn_sample_20000')

        assert(len(act) > 0)
        assert(len(act) == len(avi))
        assert(len(act) == len(ebo))
        assert(len(act) == len(mlp))
        assert(len(act) == len(xgb))
        assert(len(act) == len(min_sel))
        assert(len(act) == len(spn1000))
        assert(len(act) == len(spn10000))
        assert(len(act) == len(spn20000))
        assert(sum(avi) > 0)
        assert(sum(ebo) > 0)
        assert(sum(min_sel) > 0)
        assert(sum(mlp) > 0)
        assert(sum(xgb) > 0)
        assert(sum(spn1000) > 0)
        assert(sum(spn10000) > 0)
        assert(sum(spn20000) > 0)


if __name__ == '__main__':
    pytest.main(['test_result.py'])
