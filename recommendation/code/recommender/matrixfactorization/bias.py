'''
Created on Dec 8, 2014

@author: xuyang06
'''

import numpy
from collections import defaultdict
import copy
"""
@INPUT:
    R     : a matrix to be factorized, dimension N x M
    P     : an initial matrix of dimension N x K
    Q     : an initial matrix of dimension M x K
    K     : the number of latent features
    steps : the maximum number of steps to perform the optimisation
    alpha : the learning rate
    beta  : the regularization parameter
@OUTPUT:
    the final matrices P and Q
"""

def average_rating(R):
    rating_total = 0.0
    total_num = 0
    for i in xrange(len(R)):
        for j in xrange(len(R[0])):
            if R[i][j] > 0:
                rating_total += R[i][j]
                total_num += 1
    return rating_total/total_num
    
def matrix_factorization(R, P, Q, B_p, B_q, average_rating, K, steps=500, alpha=0.0002, beta=0.02):
    Q = Q.T
    for step in xrange(steps):
        for i in xrange(len(R)):
            for j in xrange(len(R[i])):
                if R[i][j] > 0:
                    eij = R[i][j] - numpy.dot(P[i,:],Q[:,j]) - B_p[i] - B_q[j] - average_rating
                    for k in xrange(K):
                        P[i][k] = P[i][k] + alpha * ( eij * Q[k][j] - beta * P[i][k])
                        Q[k][j] = Q[k][j] + alpha * ( eij * P[i][k] - beta * Q[k][j])
        B_p_tmp = copy.deepcopy(B_p)
        for i in xrange(len(R)):
            B_p_i_error = 0.0
            for j in xrange(len(R[i])):
                if R[i][j] > 0:
                    eij = R[i][j] - numpy.dot(P[i,:],Q[:,j]) - B_p[i] - B_q[j] - average_rating
                    B_p_i_error += eij
            B_p_tmp[i] = B_p[i] + alpha * ( B_p_i_error - beta * B_p[i])
        
        for j in xrange(len(R[0])):
            B_q_j_error = 0.0
            for i in xrange(len(R)):
                if R[i][j] > 0:
                    eij = R[i][j] - numpy.dot(P[i,:],Q[:,j]) - B_p[i] - B_q[j] - average_rating
                    B_q_j_error += eij
            B_q[j] = B_q[j] + alpha * ( B_q_j_error - beta * B_q[j])
        B_p = B_p_tmp
        
        eR = numpy.dot(P,Q)
        e = 0
        for i in xrange(len(R)):
            for j in xrange(len(R[i])):
                if R[i][j] > 0:
                    e = e + pow(R[i][j] - numpy.dot(P[i,:],Q[:,j]) - B_p[i] - B_q[j] - average_rating, 2)
                    for k in xrange(K):
                        e = e + (beta) * ( pow(P[i][k],2) + pow(Q[k][j],2) )
        for i in xrange(len(R)):
            e = e + (beta) * ( pow(B_p[i],2) )
        for j in xrange(len(R[0])):
            e = e + (beta) * ( pow(B_q[j],2) )
        if e < 0.001:
            break
    return B_p, B_q, P, Q.T, e

if __name__ == "__main__":
    #R = defaultdict(lambda:defaultdict(lambda:None))
    R = [
         [5,3,0,1],
         [4,0,0,1],
         [1,1,0,5],
         [1,0,0,4],
         [0,1,5,4],
        ]

    R = numpy.array(R)

    N = len(R)
    M = len(R[0])
    K = 2
    
    P = numpy.random.rand(N,K)
    Q = numpy.random.rand(M,K)
    B_p = numpy.random.rand(N,1)
    B_q = numpy.random.rand(M,1)
    average_rating = average_rating(R)
    B_p, B_q, nP, nQ, e = matrix_factorization(R, P, Q, B_p, B_q, average_rating, K)
    print average_rating
    print B_p
    print B_q
    print nP
    print nQ
    print e