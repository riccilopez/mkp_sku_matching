import re
import numpy as np
import functools
from scipy.spatial.distance import pdist, squareform, jaccard
from collections.abc import Iterable
import textdistance
from fuzzywuzzy import fuzz
from modules.normalize_text import extract_units

def geo_mean_overflow(iterable: Iterable):
    return np.exp(np.log(iterable).mean())

def iter_to_string_decorator(func: callable):
    '''
    Decorates a text distance function to
    take lists as input
    '''
    @functools.wraps(func)
    def wrapper(a, b):
        if not isinstance(a, str):
            a = a[0]
        if not isinstance(b, str):
            b = b[0]
        dist = func(a, b)
        return dist 
    return wrapper

@iter_to_string_decorator
def levenshtein_dist_ratio(a: str, b: str) -> float:  
    '''Computes the Levenshtein ratio (character-based) 
       between strings `a` and `b`'''
    dist = (100 - fuzz.token_sort_ratio(a, b)) * 0.01
    return dist

@iter_to_string_decorator
def dice_sorensen_dist_ratio(a: str, b: str) -> float:
    '''Computes the Sorensen-Dice ratio (token-based) 
       between strings `a` and `b`'''
    dist = textdistance\
                .Sorensen(qval = 1, as_set = True)\
                .normalized_distance(a, b)
    return dist

def remove_units(s_clean: str) -> Iterable:
    '''Extracts the measure units from a cleaned string
    '''
    text = re.sub(r'\d+[ml|lt|pz|g|oz|kg]+(\s|$)', '', s_clean).strip()
    return text


def levenshtein_and_dice_ratio(a: str, b: str, dice_weight: float = 0.8) -> float: 
    '''Computes the average distance ratio between 
       strings `a` and `b` using the 
       Levenshtein and Sorensen-Dice ratios
       '''
    # normalized Levenshtein distance
    a = remove_units(a)
    b = remove_units(b)
    dist_lev  = levenshtein_dist_ratio(a, b)
    dist_dice = dice_sorensen_dist_ratio(a, b)
    return np.mean([dist_lev * (2 - dice_weight), dist_dice * dice_weight])

def jaccard_similarity(A: Iterable, B: Iterable) -> float:
    '''Compute the Jaccard similarity between
    two lists
    '''
    card_AnB = len(list(set(A) & set(B)))
    card_AuB = len(set(A + B)) 
    jaccard_dist = 0
    if card_AuB > 0:
        jaccard_dist = np.divide(card_AnB, card_AuB)
    # card_AnB = 2 * len(list(set(A) & set(B)))
    # card_AuB = len(A) + len(B)
    # jaccard_dist = 0
    # if card_AuB > 0:
    #     jaccard_dist = np.divide(card_AnB, card_AuB)
    return jaccard_dist 

def jaccard_distance(a: str, b: str) -> float:
    '''Computes the Jaccard distance between the set
    of units of two given strings, `a` and `b`
    '''
    A = extract_units(a)
    B = extract_units(b)
    similarity = jaccard_similarity(A, B)
    dist = 1 - similarity
    return dist