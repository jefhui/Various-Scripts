# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 15:53:52 2021

@author: jhui
"""

import pyodbc
import pandas as pd
import numpy as np
import jellyfish
import time
import threading
import queue
import urllib
from sqlalchemy import create_engine

# Import data from sql
# Parameters
server = 'vhkpdsql02'
db = 'Starlight'

#Create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

print("Getting data from SQL...")
time_start_sql = time.time()
sql = """select * from Crystal_JE_Combined"""
df = pd.io.sql.read_sql(sql, conn)
print("Finished getting data from SQL, time used: "+ str(time.time()-time_start_sql))

df.fillna("",inplace=True)
df["Amount USD"] = df["Amount USD"].apply(lambda x: float(x) if x!="" else 0)
#df_save = df
df = df.applymap(lambda s:s.lower() if type(s) == str else s)
for i in df.columns:
    df[i] = df[i].astype(str).str.replace('[^0-9a-zA-Z]+',';')

# Import Search Term
df_term_0 = pd.read_csv(r"C:\Users\jhui\Desktop\Python_local\data\search_term_new.csv", engine='python')
df_term_1 = pd.read_csv(r"C:\Users\jhui\Desktop\Python_local\data\search_term_new_1.csv", engine='python')

frames = [df_term_0,df_term_1]
df_term = pd.concat(frames).reset_index(drop=True)
df_term = df_term.applymap(lambda s:s.lower() if type(s) == str else s)

def max_sim(word,term = df_term):
    temp_ls = list()
    temp_ls_word = list()
    temp_ls_word = word.split(sep=";")
    #temp_ls = [jellyfish.jaro_distance(word,x) for x in term["Proposed Search Terms"]]
    threshold = 0.9
    result = ""
    temp_str = ""
    temp_sim = ""
    temp_term = ""
    for item in temp_ls_word:
        temp_ls = [jellyfish.jaro_winkler_similarity(item,x) for x in term["Proposed Search Terms"]]
        if np.max(temp_ls) >= threshold:
            for i in range(len(temp_ls)):
                if temp_ls[i] >= threshold:
                    if temp_str != "":
                        temp_str += ";" + str(item)
                    else:
                        temp_str += str(item)
                    if temp_sim != "":
                        temp_sim += ";" + str(round(temp_ls[i], 2))
                    else:
                        temp_sim += str(round(temp_ls[i], 2))
                    if temp_term != "":
                        temp_term += ";" + term["Proposed Search Terms"][i]
                    else:
                        temp_term += term["Proposed Search Terms"][i]
                    #         [original word, str1;str2 , maxsim1;maxsim2, searchterm1;searchterm2]
                    result = [word, temp_str, temp_sim, temp_term]
    
    return result

df = df.reset_index(drop=False)
df_final = pd.DataFrame().reindex_like(df).astype('object')


start_time = time.time()

#========================================================================== PARALLEL
class MyThread(threading.Thread):
  def __init__(self, rank, queue,lock):
    threading.Thread.__init__(self)
    self.queue = queue
    self.rank = rank
    self.lock = lock

  def run(self):

    while self.queue.qsize() > 0:
        msg = self.queue.get()

        name_grp_idx = msg["index"]
        for j in range(len(msg)):
            if j == 5 or j == 14:
                df_final.at[name_grp_idx,df_final.columns[j]] = max_sim(str(msg[j]))
        if name_grp_idx%10000==0:
            print(name_grp_idx)
            print(time.time()-start_time)
#=========================================================== QUEUE
print("Queuing")

# build queue
my_queue = queue.Queue()

# put data in queue
for i in range(len(df)):
  my_queue.put(df.loc[i])

print("Finished Queuing, time used: "+ str(time.time()-start_time))
#=========================================================== QUEUE
lock = threading.Lock()

print("Calculating similarity")
num_th = 4
#  4 threads
threads = []
for i in range(num_th):
  threads.append(MyThread(i, my_queue,lock))
  threads[i].start()

# wrap up all thread
for i in range(num_th):
  threads[i].join()

print("Done.")

#========================================================================== PARALLEL

end_time = time.time()
print(end_time-start_time)

df_final.fillna("",inplace=True)
df_final = df_final[df_final.columns[1:]]


r'''
print("Exporting result to csv...")
df_final.head(1000000).to_csv(r"C:\Users\jhui\Desktop\Python_local\result\demo_0611.csv")
df_final.tail(600000).to_csv(r"C:\Users\jhui\Desktop\Python_local\result\demo_1_0611.csv")
print("Finished")
'''


# Summary

print("Getting stat...")
# stat
stat_num_hit_supplier = len(df_final[df_final["Supplier"].str.len()>0])
stat_num_hit_tran_text = len(df_final[df_final["Trans. Text"].str.len()>0])
stat_num_hit_both = len(df_final[(df_final["Supplier"].str.len()>0) & (df_final["Trans. Text"].str.len()>0)])
stat = {"Stat":["Supplier Hit","Trans. Text Hit","Both hit"],\
        "Count":[stat_num_hit_supplier,stat_num_hit_tran_text,stat_num_hit_both]}
# to df
df_stat = pd.DataFrame(stat)


# distinct key word hit
dis_sup = list()
for i in df_final["Supplier"]:
    if i != "":
        if i[3] not in dis_sup:
            dis_sup.append(i[3])

dis_tran_txt = list()
for i in df_final["Trans. Text"]:
    if i != "":
        if i[3] not in dis_tran_txt:
            dis_tran_txt.append(i[3])
# join two list            
dis_both = dis_sup+dis_tran_txt
# as df
df_term_hit =  pd.DataFrame (dis_both,columns=['Key Term Hit'])
# dedup
df_term_hit = df_term_hit.assign(terms_deflat=df_term_hit['Key Term Hit'].str.split(';')).explode('terms_deflat')
df_term_hit = df_term_hit[df_term_hit.columns[1:]]
df_term_hit = df_term_hit[(df_term_hit['terms_deflat'].duplicated(keep='first')==False)]
# add count col
df_term_hit["sup_hit"] = 0
df_term_hit["tran_text_hit"] = 0
# count hit
for j in df_final["Supplier"]:
    if j !="":
        for k in j[3].split(";"):
             df_term_hit.loc[df_term_hit['terms_deflat']==k,"sup_hit"] +=1
for j in df_final["Trans. Text"]:
   if j !="":
       for k in j[3].split(";"):
            df_term_hit.loc[df_term_hit['terms_deflat']==k,"tran_text_hit"] +=1     

df_notnull = df_final[(df_final["Supplier"].str.len()>0) | (df_final["Trans. Text"].str.len()>0)]
df_notnull["Terms hit"] = ""
df_notnull = df_notnull.reset_index(drop= False)
for i in range(len(df_notnull)):   
    if df_notnull["Supplier"][i] == "":
        df_notnull["Terms hit"][i] = df_notnull["Trans. Text"][i][3]
    elif df_notnull["Trans. Text"][i] == "":
        df_notnull["Terms hit"][i] = df_notnull["Supplier"][i][3]
    else:
        df_notnull["Terms hit"][i] = df_notnull["Supplier"][i][3] + ";" + df_notnull["Trans. Text"][i][3]
print("Finished getting stat")                    

print("Exporting summary and stat...")
with pd.ExcelWriter(r"C:\Users\jhui\Desktop\Python_local\result\summary_0611.xlsx") as writer:  
    df_notnull.to_excel(writer, sheet_name = "Summary", index=False)
    worksheet = writer.sheets['Summary']
    df_term_hit.to_excel(writer, sheet_name = "By terms", index=False)
    worksheet = writer.sheets['By terms']
    df_stat.to_excel(writer, sheet_name = "Stat", index=False)
    worksheet = writer.sheets['Stat']
    
writer.save()
writer.close()
print("Done")

'''
quoted = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

df.to_sql('fuzzy_summary_all', con = engine, if_exists='replace',index=False)

result = engine.execute('SELECT COUNT(*) FROM [dbo].[fuzzy_summary_all]')
result.fetchall()

'''

'''
df_supp = df[["index","Amount USD"]]
quoted = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

df_supp.to_sql('fuzzy_summary_all_amount', con = engine, if_exists='replace',index=False)

result = engine.execute('SELECT COUNT(*) FROM [dbo].[fuzzy_summary_all_amount]')
result.fetchall()
'''
