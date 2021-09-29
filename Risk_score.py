# -*- coding: utf-8 -*-lma
"""
Created on Mon Feb 22 17:39:04 2021

@author: jhui
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc 
import urllib

# Import data from sql
# Parameters
server = 'vhkpdsql02'
db = 'PorcelainDB'

#Create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

sql = """select * from dbo.final_table"""
df = pd.io.sql.read_sql(sql, conn)

col_id = ["UNIQUE ID"]
col_high_risk = ["SSN",'TAX ID','TaxpayerIdentificationNumbersCountryName','PASSPORT NUMBER',"DRIVER'S LICENSE NUMBERS"
                 ,'StateIdentificationCardNumber','StateNationCountryIssuedIDcard','STATE IDENTIFICATION CARD NUMBER YES NO'
                 ,'ALIEN REGISTRATION NUMBER','OTHER GOVERNMENT ISSUED ID','TypeofOtherID','TRIBAL IDENTIFICATION NUMBER'
                 ,'PAYMENT CARD NUMBER','FINANCIAL ACCOUNT NUMBER','FINANCIAL ROUTING NUMBER','NATIONAL INSURANCE NUMBER']
df_risk_score = pd.DataFrame()
high_risk_score = 5
low_risk_score = 1
df.fillna("",inplace=True)
df.replace(" ","", inplace = True)
for i in df.columns:
    if (i not in col_id) and (i not in col_high_risk):
        df_risk_score[i] = np.where(df[i]=='' ,0,low_risk_score)
    if (i not in col_id) and (i in col_high_risk):
        df_risk_score[i] = np.where(df[i]=='' ,0,high_risk_score)
df_final = pd.DataFrame()
df_final["UNIQUE ID"] = df["UNIQUE ID"]
df_final['AddressPersonal'] = df['AddressPersonal']
df_final['AM_Risk_Score'] = df_risk_score.sum(axis=1)

#Export to SQL
quoted = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

df_final.to_sql('stg_consolidatedList_risk_score', con = engine, if_exists='replace',index=False)

result = engine.execute('SELECT COUNT(*) FROM [dbo].[stg_consolidatedList_risk_score]')
result.fetchall()