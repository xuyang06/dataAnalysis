'''
Created on Dec 8, 2014

@author: xuyan_000
'''
from collections import defaultdict
import random

def form_dict(data):
    user_item_dict = defaultdict(lambda:defaultdict(lambda:None))
    item_user_dict = defaultdict(lambda:defaultdict(lambda:None))
    star_total = 0.0
    item_num = 0
    for data_item in data:
        user_id = data_item['user_id']
        item_id = data_item['business_id']
        star = data_item['stars']
        user_item_dict[user_id][item_id] = star
        item_user_dict[item_id][user_id] = star
        star_total += star
        item_num += 1
    return user_item_dict, item_user_dict, star_total/item_num
        
    
def generate_random_bias(dict):
    bias_dict = defaultdict(lambda:None)
    random.seed()
    for key in dict:
        bias_dict[key] = random.random()
    return bias_dict

def generate_random_vector(dict, K):
    vector_dict = defaultdict(lambda:[])
    random.seed()
    for key in dict:
        vector_dict[key] = []
        for _ in range(K):
            vector_dict[key].append(random.random())
    return vector_dict



