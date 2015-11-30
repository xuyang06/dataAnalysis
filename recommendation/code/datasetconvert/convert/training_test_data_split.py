'''
Created on Nov 21, 2014

@author: xuyan_000
'''

import random

review_path_str = 'D:/study/recommendation/yelp/dataset/yelp_academic_dataset_review.csv'

training_review_path_str = 'D:/training_review.csv'
#validation_review_path_str = 'D:/validation_review.csv'
test_review_path_str = 'D:/test_review.csv'


def splitFile():
    random.seed()
    review_f = open(review_path_str, 'r+')
    training_review_f = open(training_review_path_str, 'w')
    #validation_review_f = open(validation_review_path_str, 'w')
    test_review_f = open(test_review_path_str, 'w')
    first_line = True
    user_id_dict = {}
    for line in review_f:
        if first_line == True:
            training_review_f.writelines(line)
            #validation_review_f.writelines(line)
            test_review_f.writelines(line)
            first_line = False
        else:
            review_list = line.split(',')
            if review_list[0] not in user_id_dict:
                user_id_dict[review_list[0]] = []
                user_id_dict[review_list[0]].append(line)
            else:
                user_id_dict[review_list[0]].append(line)
    for user_id in user_id_dict:
        user_review_lines = user_id_dict[user_id]
        if len(user_review_lines) == 1:
            training_review_f.writelines(user_review_lines[0])
        else:
            training_lines = []
            test_lines = []
            for line in user_review_lines:
                rand_i = random.random()
                if rand_i < 0.9:
                    training_lines.append(line)
                else:
                    test_lines.append(line)
            if test_lines != [] and training_lines != []:
                for line in training_lines:
                    training_review_f.writelines(line)
                for line in test_lines:
                    test_review_f.writelines(line)
            else:
                for line in user_review_lines:
                    training_review_f.writelines(line)
    review_f.close()
    training_review_f.close()
    #validation_review_f.close()
    test_review_f.close()

if __name__ == '__main__':
    splitFile()