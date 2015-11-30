# -*- coding: utf-8 -*-
"""Convert the Yelp Dataset Challenge dataset from json format to csv.

For more information on the Yelp Dataset Challenge please visit http://yelp.com/dataset_challenge

"""
import argparse
import collections
import csv
import simplejson as json
import pickle
import os.path
import conversion

userIdConversionPathStr = 'D:/user_id.p'
businessIdConversionPathStr = 'D:/business_id.p'

def read_pickle():
    userId_f = None
    businessId_f = None
    userId_c = None
    businessId_c = None
    if os.path.exists(userIdConversionPathStr):
        userId_f = file(userIdConversionPathStr, 'r+')
        userId_c = pickle.load(userId_f)
    else:
        userId_f = file(userIdConversionPathStr, 'w')
        userId_c = conversion.userIdConversion()
    if os.path.exists(businessIdConversionPathStr):
        businessId_f = file(businessIdConversionPathStr, 'r+')
        businessId_c = pickle.load(businessId_f)
    else:
        businessId_f = file(businessIdConversionPathStr, 'w')
        businessId_c = conversion.businessIdConversion()
    return userId_f, userId_c, businessId_f, businessId_c
    
def write_pickle(userId_f, userId_c, businessId_f, businessId_c):
    pickle.dump(userId_c, userId_f)
    pickle.dump(businessId_c, businessId_f)
    userId_f.close()
    businessId_f.close()


def read_file(file_path):
    """Read in the json dataset file and return a list of python dicts."""
    file_contents = []
    column_names = set()
    with open(file_path) as fin:
        for line in fin:
            line_contents = json.loads(line)
            column_names.update(
                    set(get_column_names(line_contents).keys())
                    )
            file_contents.append(line_contents)
    return file_contents, column_names

def get_column_names(line_contents, parent_key=''):
    """Return a list of flattened key names given a dict.

    Example:

        line_contents = {
            'a': {
                'b': 2,
                'c': 3,
                },
        }

        will return: ['a.b', 'a.c']

    These will be the column names for the eventual csv file.

    """
    column_names = []
    for k, v in line_contents.iteritems():
        column_name = "{0}.{1}".format(parent_key, k) if parent_key else k
        if isinstance(v, collections.MutableMapping):
            column_names.extend(
                    get_column_names(v, column_name).items()
                    )
        else:
            column_names.append((column_name, v))
    return dict(column_names)

def get_nested_value(d, key):
    """Return a dictionary item given a dictionary `d` and a flattened key from `get_column_names`.
    
    Example:

        d = {
            'a': {
                'b': 2,
                'c': 3,
                },
        }
        key = 'a.b'

        will return: 2
    
    """
    if '.' not in key:
        if key not in d:
            return None
        return d[key]
    base_key, sub_key = key.split('.', 1)
    if base_key not in d:
        return None
    sub_dict = d[base_key]
    return get_nested_value(sub_dict, sub_key)

def get_row(line_contents, column_names, userId_c, businessId_c):
    """Return a csv compatible row given column names and a dict."""
    row = []
    for column_name in column_names:
        line_value = get_nested_value(
                        line_contents,
                        column_name,
                        )
        if column_name == 'user_id':
            line_value = userId_c.getUserId(line_value)
        elif column_name == 'business_id':
            line_value = businessId_c.getBusinessId(line_value)
        elif column_name == 'date':
            line_value = conversion.getDateTime(line_value)
        if isinstance(line_value, unicode):
            row.append('{0}'.format(line_value.encode('utf-8')))
        elif line_value is not None:
            row.append('{0}'.format(line_value))
        else:
            row.append('')
    #print 'column_names: ' + str(column_names)
    #print 'row: ' + str(row)
    return row

def write_file(file_path, file_contents, column_names, userId_c, businessId_c):
    """Create and write a csv file given file_contents of our json dataset file and column names."""
    csv_file = csv.writer(open('file_path', 'wb+'))
    with open(file_path, 'wb+') as fin:
        csv_file = csv.writer(fin)
        csv_file.writerow(list(column_names))
        for line_contents in file_contents:
            csv_file.writerow(get_row(line_contents, column_names, userId_c, businessId_c))



if __name__ == '__main__':
    """Convert a yelp dataset file from json to csv."""
    userId_f, userId_c, businessId_f, businessId_c = read_pickle()
    
    parser = argparse.ArgumentParser(
            description='Convert Yelp Dataset Challenge data from JSON format to CSV.',
            )

    parser.add_argument(
            'json_file',
            type=str,
            help='The json file to convert.',
            )

    args = parser.parse_args()

    json_file = args.json_file
    csv_file = '{0}.csv'.format(json_file.split('.json')[0])
    file_contents, column_names = read_file(json_file)
    #print column_names
    #column_names = ['business_id', 'user_id', 'stars', 'text', 'date']
    column_names = ['business_id', 'user_id', 'text', 'likes', 'date']
    write_file(csv_file, file_contents, column_names, userId_c, businessId_c)
    write_pickle(userId_f, userId_c, businessId_f, businessId_c)

# if __name__ == '__main__':
#     """Convert a yelp dataset file from json to csv."""
# 
#     parser = argparse.ArgumentParser(
#             description='Convert Yelp Dataset Challenge data from JSON format to CSV.',
#             )
# 
#     parser.add_argument(
#             'json_file',
#             type=str,
#             help='The json file to convert.',
#             )
# 
#     args = parser.parse_args()
# 
#     json_file = args.json_file
#     csv_file = '{0}.csv'.format(json_file.split('.json')[0])
#     file_contents, column_names = read_file(json_file)
#     #print column_names
#     column_names = ['business_id', 'user_id', 'text', 'likes', 'date']
#     write_file(csv_file, file_contents, column_names)
