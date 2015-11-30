'''
Created on Nov 20, 2014

@author: xuyan_000
'''

from datetime import datetime
import time

def getDateTime(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return time.mktime(date_obj.timetuple())

class userIdConversion(object):
    def __init__(self):
        self.str_id_dict = {}
        self.longest_id = 1
        
    def getUserId(self, user_str):
        if user_str in self.str_id_dict:
            return self.str_id_dict[user_str]
        else:
            user_id = self.longest_id
            self.str_id_dict[user_str] = user_id
            self.longest_id += 1
            return user_id
    
    #def setUserId(self, user_str):
        
        
class businessIdConversion(object):
    def __init__(self):
        self.str_id_dict = {}
        self.longest_id = 1
        
    def getBusinessId(self, business_str):
        if business_str in self.str_id_dict:
            return self.str_id_dict[business_str]
        else:
            business_id = self.longest_id
            self.str_id_dict[business_str] = business_id
            self.longest_id += 1
            return business_id
    
    #def setUserId(self, user_str):
        

if __name__ == '__main__':
    date_str = '2007-05-17'
    print getDateTime(date_str) 