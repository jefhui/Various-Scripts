# -*- coding: utf-8 -*-
"""
Created on Thu May 13 13:22:20 2021

@author: jhui
"""

import pandas as pd
import numpy as np
from collections import Counter
import pyodbc 

def domain(x):
        try:
            return "@" + str(x).split("@")[1]
        except:
            return ""

    
def is_employee(x):
    if x != "":
        try:
            domain_str = "@" + str(x).split("@")[1]
            try:
                domain_list.index(domain_str)
                return domain_str
            except:
                return ""
        except:
            return ""
        
# Import data from sql
# Parameters
server = 'vhkpdsql02'
db = 'PorcelainDB_Phase2'

#Create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

sql = """select * from dbo.rpt_consolidated_all"""
df = pd.io.sql.read_sql(sql, conn)

# load domain list
df_domain = pd.read_excel('//VHKPDFS01/DA_Project/Project_Porcelain/Data/domain_list_20210316.xlsx')
domain_list = list(df_domain.iloc[:,0])

# add @ for each domain
domain_list = ["@" + domain for domain in domain_list]

df_save = df
# all to lower case
df = df.applymap(lambda s:s.lower() if type(s) == str else s)
# remove bad character
df['EMAIL ADDRESS (PERSONAL)'] = df['EMAIL ADDRESS (PERSONAL)'].str.replace("]","")
df['EMAIL ADDRESS (WORK)'] = df['EMAIL ADDRESS (WORK)'].str.replace("]","")
df['EMAIL ADDRESS (PERSONAL)'] = df['EMAIL ADDRESS (PERSONAL)'].str.replace("/","")
df['EMAIL ADDRESS (WORK)'] = df['EMAIL ADDRESS (WORK)'].str.replace("/","")
df['EMAIL ADDRESS (PERSONAL)'] = df['EMAIL ADDRESS (PERSONAL)'].str.replace("\\","")
df['EMAIL ADDRESS (WORK)'] = df['EMAIL ADDRESS (WORK)'].str.replace("\\","")
df['EMAIL ADDRESS (PERSONAL)'] = df['EMAIL ADDRESS (PERSONAL)'].str.replace("[","")
df['EMAIL ADDRESS (WORK)'] = df['EMAIL ADDRESS (WORK)'].str.replace("[","")
df['EMAIL ADDRESS (PERSONAL)'] = df['EMAIL ADDRESS (PERSONAL)'].str.replace(">","")
df['EMAIL ADDRESS (WORK)'] = df['EMAIL ADDRESS (WORK)'].str.replace(">","")
df['EMAIL ADDRESS (PERSONAL)'] = df['EMAIL ADDRESS (PERSONAL)'].str.replace(",","")
df['EMAIL ADDRESS (WORK)'] = df['EMAIL ADDRESS (WORK)'].str.replace(",","")

# filter if in domain list
pattern = '|'.join(domain_list)

# deflat
df = df[['UNIQUE ID','EMAIL ADDRESS (PERSONAL)','EMAIL ADDRESS (WORK)']]
df = df.assign(personal_email_deflat=df['EMAIL ADDRESS (PERSONAL)'].str.split(';')).explode('personal_email_deflat')
df = df.assign(work_email_deflat=df['EMAIL ADDRESS (WORK)'].str.split(';')).explode('work_email_deflat')

df['is_employee_personal'] = df.apply(lambda x: is_employee(x['personal_email_deflat']), axis=1)
df['is_employee_work'] = df.apply(lambda x: is_employee(x['work_email_deflat']), axis=1)

df = df[['UNIQUE ID','EMAIL ADDRESS (PERSONAL)','EMAIL ADDRESS (WORK)','is_employee_personal', 'is_employee_work','personal_email_deflat','work_email_deflat']]
df = df[(df['is_employee_personal']=="")|(df['is_employee_work']=="")]

df["not_em_domain_personal"] = np.where(df['is_employee_personal']=="",df['personal_email_deflat'].apply(domain),"")
df["not_em_domain_work"] = np.where(df['is_employee_work']=="",df['work_email_deflat'].apply(domain),"")

count_1 = Counter(df['not_em_domain_personal'])
count_2 = Counter(df['not_em_domain_work'])