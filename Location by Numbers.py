# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 15:18:38 2020

@author: jhui
"""


import pandas as pd
import numpy as np
import pyodbc 


# Import data from sql
# Parameters
server = 'vhkpdsql02'
db = 'PorcelainDB'

#Create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

sql = """select * from src_ConsolidatedList"""
df = pd.io.sql.read_sql(sql, conn)
df = df[["UNIQUE ID","AddressPersonal","AddressWork","Telephone No. Personal", "Telephone No. Work", "Location"]]

def location(x):
    if ((len(x)==11) and (str(x)[0:3]=='852')):
        return "HK"
    elif ((len(x)==12) and (str(x)[0:2]=='44')):
        return "UK"
    elif ((len(x)==11) and (str(x)[0]=='1')):
        return "US_Canada"  
    elif ((len(x)==12) and (str(x)[0:2]=='86')):
        return "China"
    elif ((len(x)==11) and (str(x)[0]=='7')):
        return "Russia"           
    elif ((len(x)==10) and (str(x)[0:2]=='65')):
        return "Singapore"
    #elif ((len(x)==12) and (str(x)[0:2]=='49')):
    #    return "Germany"
    elif ((len(x)==12) and (str(x)[0:2]=='91')):
        return "India"
    elif ((len(x)==12) and (str(x)[0:2]=='39')):
        return "Italy"
    elif ((len(x)==10) and (str(x)[0:2]=='82')):
        return "Korea"
    elif ((len(x)==12) and (str(x)[0:3]=='886')):
        return "Taiwan"
    elif ((len(x)==12) and (str(x)[0:3]=='971')):
        return "UAE"
    elif ((len(x)==12) and (str(x)[0:3]=='966')):
        return "Saudi_Arabia"
    elif ((len(x)==12) and (str(x)[0:2]=='81')):
        return "Japan"    
    else:
        return ""

    
df['LocationByNumber Work'] = df["Telephone No. Work"].apply(lambda x:location(x))

df['LocationByNumber Personal'] = df["Telephone No. Personal"].apply(lambda x:location(x))

df_review = df[(df['LocationByNumber Work'].str.len()>=2) | (df['LocationByNumber Personal'].str.len()>=2)]


df_summary = df_review[["UNIQUE ID","LocationByNumber Work"]].groupby(["LocationByNumber Work"]).count()
df_summary["personal"] = df_review[["UNIQUE ID","LocationByNumber Personal"]].groupby(["LocationByNumber Personal"]).count()
df_summary.columns = ["work","personal"]
df_summary = df_summary.rename_axis(index=" ",columns="")


country_list = [country for country in df_summary.index]
country_list.remove("")

df_list = {}
for country in country_list:
    df_list["df_review_work_"+country] = df_review[df['LocationByNumber Work']==country]
    df_list["df_review_personal_"+country] = df_review[df['LocationByNumber Personal']==country]

# write df to excel
with pd.ExcelWriter('//VHKPDFS01/DA_Project/Project_Porcelain/Data/Project Porcelain - Number To Location.xlsx') as writer:  
    df_summary.to_excel(writer, sheet_name = "Summary", index = True)
    worksheet = writer.sheets['Summary']
    # fit column width
    for idx, col in enumerate(df_summary):
                series = df_summary[col]
                max_len = max((series.astype(str).map(len).max(),  # len of largest item
                               len(str(series.name))  # len of column name/header
                               )) + 1
                worksheet.set_column(idx, idx, max_len)
    for df_ in df_list.keys():
        try:
            df_list[df_].to_excel(writer, sheet_name=df_)
            worksheet = writer.sheets[df_list[df_]]
            # fit column width
            for idx, col in enumerate( df_list[df]):
                series =  df_list[df_][col]
                max_len = max((series.astype(str).map(len).max(),  # len of largest item
                               len(str(series.name))  # len of column name/header
                               )) + 1
                worksheet.set_column(idx+1, idx+1, max_len)
        except:
            pass
       
writer.save()
writer.close()