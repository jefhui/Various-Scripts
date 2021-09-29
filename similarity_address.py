# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 09:59:44 2020

@author: jhui
"""

import pandas as pd
import numpy as np
from Levenshtein import *
import pyodbc 
from itertools import chain
from fuzzywuzzy import fuzz
from sqlalchemy import create_engine
import urllib
import unicodedata
import string
import math
import time


def append_value(dict_obj, key, value):
    # Check if key exist in dict or not
    if key in dict_obj:
        # Key exist in dict.
        # Check if type of value of key is list or not
        if not isinstance(dict_obj[key], list):
            # If type is not list then make it list
            dict_obj[key] = [dict_obj[key]]
        # Append the value in list
        dict_obj[key].append(value)
    else:
        # As key is not in dict,
        # so, add key-value pair
        dict_obj[key] = value

# function to return key for any value 
def get_key(val): 
    for key, value in sim_add.items(): 
         if val in value: 
             return key 
  
    return False

def jaccard_similarity(list1, list2):
    s1 = set(list1)
    s2 = set(list2)
    return float(len(s1.intersection(s2)) / len(s1.union(s2)))

# Import data from sql
# Parameters
server = 'vhkpdsql02'
db = 'PorcelainDB'

#Create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

sql = """select * from dbo.stg_PersonTracker"""
df = pd.io.sql.read_sql(sql, conn)

# Data Cleanse
df_address = df[['ArtifactID','FirstName','MiddleName','LastName','AM_AddressPersonal_Clean']]

df_address["First Name"] = df["FirstName"].fillna('')
df_address["Middle Name"] = df["MiddleName"].fillna('')
df_address["Last Name"] = df["LastName"].fillna('')
# convert whole df to lower case
df_address = df_address.applymap(lambda s:s.lower() if type(s) == str else s)
# encoding decoding problem
df_address['First Name'] = df_address['First Name'].str.replace(u'\xa0','')
df_address['Middle Name'] = df_address['Middle Name'].str.replace(u'\xa0','')
df_address['Last Name'] = df_address['Last Name'].str.replace(u'\xa0','')
df_address['AddressPersonal'] = df_address['AM_AddressPersonal_Clean'].str.replace('[^\w\s]',' ')
df_address['AddressPersonal'] = df_address['AM_AddressPersonal_Clean'].str.replace('  ',' ')

# minimize loops needed
df_address = df_address[df_address['AddressPersonal'].notnull()]
df_address = df_address[df_address['AddressPersonal'].str.len()>1]
df_address = df_address[df_address['First Name'].str.contains('unknown',na=False)==False]
df_address = df_address[df_address['Middle Name'].str.contains('unknown',na=False)==False]
df_address = df_address[df_address['Last Name'].str.contains('unknown',na=False)==False]

df_address = df_address.reset_index()

sim_add = {}

# is the string english
def is_eng(s):
    for c in s:
        cat = unicodedata.category(c)
        if (cat not in ('Ll','Lu', 'Nd')) and (c!=" ") and (c not in string.punctuation):
            return False
    return True

# split by word if english, by character if chinese
df_address["First Name"] = df_address['First Name'].apply(lambda x: x.split()if is_eng(x)==True else list(x))
df_address["Middle Name"] = df_address['Middle Name'].apply(lambda x: x.split()if is_eng(x)==True else list(x))
df_address["Last Name"] = df_address['Last Name'].apply(lambda x: x.split()if is_eng(x)==True else list(x))
# full set of name
df_address["Full Name Set"] = df_address["First Name"]+df_address["Middle Name"]+df_address["Last Name"]

start_time = time.time()
print(start_time)
for i in range(len(df_address)):
    # if a==b, b==c, a==c already in the list, no need to loop c
    #if i not in list(chain.from_iterable(list(sim_add.values()))):
    for j in range(i,len(df_address)):
        if i==j:
            continue
        else:
            try:
                # token set ratio is not commutative
                dis = fuzz.token_set_ratio(df_address['AddressPersonal'][i],df_address["AddressPersonal"][j])
                dis_1 = fuzz.token_set_ratio(df_address['AddressPersonal'][j],df_address["AddressPersonal"][i])
                # only match address if name is similar
                dis_name = jaccard_similarity(df_address["Full Name Set"][i],df_address["Full Name Set"][j])
            except:
                continue
            if ((dis >= 85) or (dis_1 >= 85)) and (dis_name==1):
                # if only j is new duplicate
                if (df_address['ArtifactID'][i] in list(chain.from_iterable(list(sim_add.values())))) and (df_address['ArtifactID'][j] not in list(chain.from_iterable(list(sim_add.values())))):
                    append_value(sim_add, get_key((df_address['ArtifactID'][i])), (df_address['ArtifactID'][j]))
                # If both is new duplicate
                elif df_address['ArtifactID'][i] and df_address['ArtifactID'][j] not in list(chain.from_iterable(list(sim_add.values()))) :
                    sim_add.update({df_address['ArtifactID'][i]:[df_address['ArtifactID'][i],df_address['ArtifactID'][j]]})
end_time = time.time()
print(end_time-start_time)

# append to df
# add need column
df['sim_add_grp'] = 0
df['longest_add'] = ""
for i in sim_add.items():
    longest = ""
    # find longest address in same similar address group
    for j in i[1]:
        if len(df.loc[df['ArtifactID']==j,['AddressPersonal']]) > len(longest):
            longest = df.loc[df['ArtifactID']==j,['AddressPersonal']].iloc[0,0]
    # sub similar address group id and longest address
    for j in i[1]:
        df.loc[df['ArtifactID']==j,['sim_add_grp']] = i[0]
        df.loc[df['ArtifactID']==j,['longest_add']] = longest

# select only useful columns to export
df_final = df[['ArtifactID','sim_add_grp','longest_add']]

#Export to SQL
quoted = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

df_final.to_sql('stg_AddressPersonalNameGroup_check', con = engine, if_exists='replace',index=False)

result = engine.execute('SELECT COUNT(*) FROM [dbo].[stg_AddressPersonalNameGroup_check]')
result.fetchall()