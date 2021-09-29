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
import time

def risk_lvl(a,b,c,d):
    # a:High b:Medium c:Low d: No
    if d>0:
        return "No (Deceased)"
    elif (a>0) or (b>1):
        return "High"
    elif b==1:
        return "Medium"
    elif c>0:
        return "Low"
    else:
        return "No"


# Import data from sql
# Parameters
server = 'vhkpdsql02'
db = 'PorcelainDB_QC'

#Create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

print("Importing data from SQL...")

sql = "select * from dbo.rpt_consolidated_all_QCRedo"
df = pd.io.sql.read_sql(sql, conn)

col_id = ["UNIQUE ID"]
col_high_risk = ["DATE OF BIRTH",\
                 "SSN",\
                 'TAX ID', 'TAX ID COUNTRY', 'OTHER TAX INFORMATION',\
                 'PASSPORT NUMBERS/SCAN/VISA/WORK ELIGIBILITY DOCUMENTATION', 'PASSPORT EXPIRATION DATE',\
                 "DRIVER'S LICENSE NUMBERS", "DRIVER'S LICENSE NUMBER EXPIRATION DATE",\
                 'STATE IDENTIFICATION CARD NUMBER', 'STATE IDENTIFICATION CARD STATE/NATION/COUNTRY',\
                 'OTHER GOVERNMENT ISSUED ID', 'OTHER GOVERNMENT ISSUED ID TYPE',\
                 'BIRTH CERTIFICATE',\
                 'PAYMENT CARD NUMBER', 'PAYMENT CARD EXPRIATION DATE/PIN/PASSWORD/CVV/ACCESS CODE',\
                 'BANK ACCOUNT/IBAN NUMBERS', 'BANK ACCOUNT/IBAN ROUTING NUMBER/FINANCIAL INSTITUTION NAME/EXPIRATION DATE/PIN/PASSWORD',\
                 'OTHER FINANCIAL DATA',\
                 'LOGINS',\
                 'NATIONAL INSURANCE NUMBER']
col_med_risk = ['SIGNATURE',\
                'MINOR', "MINOR'S PARENT(S) NAME",\
                'CRIMINAL CONVICTIONS/OFFENSES/CRIMINAL RECORD CHECK RESULTS',\
                'MEDICAL INFORMATION',\
                'EMPLOYMENT INFORMATION',\
                'SEXUAL ORIENTATION / SEX LIFE']
col_low_risk = ['ADDRESS LINE (PERSONAL)', 'COUNTRY (PERSONAL)',\
                'TELEPHONE NUMBER (PERSONAL)',\
                'EMAIL ADDRESS (PERSONAL)',\
                'WORK ADDRESS', 'ADDRESS LINE (WORK)', 'COUNTRY (WORK)',\
                'EMAIL ADDRESS (WORK)',\
                'TELEPHONE NUMBER (WORK)', 'TELEPHONE NUMBER (WORK) (#1)',\
                'GENDER',\
                'NATIONALITY / RACE / RELIGION / ETHNIC ORIGIN']


df_risk_score = pd.DataFrame()
df.fillna("",inplace=True)
df.replace(" ","", inplace = True)

for i in range(len(col_high_risk)):
    col_high_risk[i] = col_high_risk[i].replace(" ","_")
for i in range(len(col_med_risk)):
    col_med_risk[i] = col_med_risk[i].replace(" ","_")
for i in range(len(col_low_risk)):
    col_low_risk[i] = col_low_risk[i].replace(" ","_")

# risk score for each col
for i in df.columns:
    temp = i.replace(" ","_")
    if temp in col_high_risk:
        df_risk_score[i] = np.where(df[i]=='' ,"","H")
    elif temp in col_med_risk:
        df_risk_score[i] = np.where(df[i]=='' ,"","M")
    elif temp in col_low_risk:
        df_risk_score[i] = np.where(df[i]=='' ,"","L")
    elif temp == "DECEASED":
        df_risk_score[i] = np.where(df[i]=='' ,"","N")
    else:
        df_risk_score[i] = np.where(df[i]=='' ,"","")

# risk level final        
df_final = pd.DataFrame()
df_final = df_risk_score
df_final = df_final.apply(pd.Series.value_counts, axis=1)
df_final["UNIQUE ID"] = df["UNIQUE ID"]
df_final.fillna(0,inplace=True)
df_final["Risk Level"] = df_final.apply(lambda x: risk_lvl(x["H"],x["M"],x["L"],x["N"]), axis = 1)
df_final = df_final[["UNIQUE ID","Risk Level"]]
np.isin(df_risk_score,"H")

print("Getting detail...")
start_time = time.time()

risk_level = ["H","M","L","N"]
for risk in risk_level:
    df_risk_detail = pd.DataFrame(np.isin(df_risk_score,risk))
    df_risk_detail.columns = df_risk_score.columns
    for i in df_risk_detail.columns:
        df_risk_detail[i] = np.where(df_risk_detail[i]==True, i.replace(" ","_"), "")
    temp_detail = df_risk_detail.to_string(header=False,index=False,index_names=False).split('\n')    
    df_final[risk+"_detail"] = [';'.join(ele.split()) for ele in temp_detail]

end_time = time.time()
print("Time spent: " + str(end_time - start_time) + "seconds")


print("Exporting result to SQL...")
start_time = time.time()
#Export to SQL
quoted = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

df_final.to_sql('rpt_risk_level', con = engine, if_exists='replace',index=False)

result = engine.execute('SELECT COUNT(*) FROM [dbo].[rpt_risk_level]')
result.fetchall()

end_time = time.time()
print("Time spent: " + str(end_time - start_time) + "seconds")
print("Finished")