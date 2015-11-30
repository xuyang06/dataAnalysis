'''
Created on Dec 8, 2014

@author: xuyang06
'''

import graphlab as gl


if __name__ == '__main__':
    training_data_path = '/home/xuyang06/Dropbox/training_review.csv'
    training_data = gl.SFrame.read_csv(training_data_path, delimiter=',', column_type_hints={"stars":int})
    test_data_path = '/home/xuyang06/Dropbox/test_review.csv'
    test_data = gl.SFrame.read_csv(test_data_path, delimiter=',', column_type_hints={"stars":int})
    factorization_model = gl.recommender.factorization_recommender.create(training_data, user_id='user_id', item_id='business_id', target='stars', user_data=None, item_data=None, num_factors=10, regularization=1e-06, linear_regularization=0.0, side_data_factorization=True, nmf=False, binary_target=False, max_iterations=50, sgd_step_size=0, random_seed=0, solver='auto', verbose=True)
    factorization_model.evaluate(test_data)
    similarity_model = gl.recommender.item_similarity_recommender.create(training_data, user_id='user_id', item_id='business_id', target='stars')
    similarity_model.evaluate(test_data)
    #factorization_ model.
    