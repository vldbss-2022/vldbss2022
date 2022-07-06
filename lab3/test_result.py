import pytest
import json

class Test_simple():
    def test_lab3(self):
        json_file = 'lab3/eval/results.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
        assert(len(data) > 0)
        for item in data:
            cost_flag, card_flag = False, False
            warnings = item["warnings"]
            for warn in warnings:
                if "successfully" in warn:
                    if "selectivity" in warn:
                        card_flag = True
                    if "cost" in warn:
                        # Warning\tget cost(HashAgg_14)=1 successfully from the external model
                        begin_idx = warn.find(")=")
                        if begin_idx == -1:
                            continue
                        end_idx = begin_idx + warn[begin_idx:].find(" ")
                        if end_idx == -1:
                            continue
                        cost_str = warn[begin_idx+2:end_idx]
                        if float(cost_str) > 0:
                            cost_flag = True
            assert(cost_flag == True and card_flag == True)

if __name__ == '__main__':
    pytest.main(['test_result.py']) 