import pytest
import json
from modules.normalize_text import normalize_text
from modules.sku_matcher import get_confidence

# Read the json containing the mktp_sku and comp_sku names to evaluate and
# the expecting confidence values.
json_path = './tests/expected_matches.json'
with open(json_path, 'r') as file:
    dict_matches = json.load(file)


def get_all_conf_values():
    """
    Returns a list of 2-len tuples containing the pred_confidence and the 
    expected confidence for a given pair of mktp_sku and comp_sku names.
    """
    conf_pairs = []
    for sku_name, product_dict in dict_matches.items():
        for values in product_dict.values():
            s1 = sku_name
            s2 = values[0]
            s1, s2 = normalize_text(s1), normalize_text(s2)
            text_conf = get_confidence(s1, s2)
            expected_conf = values[1]
            assert_type = values[2]
            conf_pairs.append((text_conf, expected_conf, assert_type, sku_name))
    return conf_pairs

@pytest.mark.parametrize(
        "text_conf, expected_conf, assert_type, s1", get_all_conf_values())
def test_get_confidence(text_conf, expected_conf, assert_type, s1):
    """Evaluates the given pair of sku_names."""
    if assert_type == "==":
        assert text_conf == expected_conf, s1
    elif assert_type == "<=":
        assert text_conf <= expected_conf, s1
    elif assert_type == ">=":
        assert text_conf >= expected_conf, s1
    else:
        assert False
    

if __name__ == "__main__":
    import pytest
    pytest.main()