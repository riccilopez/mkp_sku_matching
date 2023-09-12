
import numpy as np
from modules.distance_metrics import levenshtein_and_dice_ratio, jaccard_distance_units

# TEST: Combine Jaccard with Lev-Dice distances
def sku_name_conf(a, b) -> float:
    '''Ponderates Levenshtein-Dice distance with 
    Jaccard distances for unit matching'''
    lvd_conf = 1 - levenshtein_and_dice_ratio(a, b)
    jac_dist_units = jaccard_distance_units(a, b)
    # Apply a ReLU
    pond_conf = np.max([0.0, (lvd_conf) - (jac_dist_units * 0.17)]) 
    return pond_conf


def exp_matching_value(x: float, decay: float = 1.02) -> float:
    """
    Calculate an exponential matching value for a given input.
    """
    if x == 0:
        return x
    else:
        return np.exp(1 - (1/x**decay))


def get_confidence(a: str, b: str) -> float:
    """
    Calculate the confidence score between two SKU names.
    """
    conf = sku_name_conf(a, b)
    conf = exp_matching_value(conf, decay = 1.01)
    return round(conf, 2)

