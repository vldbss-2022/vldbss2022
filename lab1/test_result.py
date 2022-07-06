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

        assert(len(act) == len(avi))
        assert(len(act) == len(ebo))
        assert(len(act) == len(mlp))
        assert(len(act) == len(xgb))
        assert(len(act) == len(min_sel))
        assert(len(act) == len(spn1000))
        assert(len(act) == len(spn10000))
        assert(len(act) == len(spn20000))

        for v in ebo:
            assert(v>=0)
        for v in mlp:
            assert(v>=0)
        for v in min_sel:
            assert(v>=0)
        for v in xgb:
            assert(v>=0)
        for v in spn1000:
            assert(v>=0)
        for v in spn10000:
            assert(v>=0)
        for v in spn20000:
            assert(v>=0)


if __name__ == '__main__':
    pytest.main(['test_result.py'])
