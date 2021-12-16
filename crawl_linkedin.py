# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 16:50:48 2021

@author: JHui
"""


from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
from datetime import date,timedelta
import json
import chromedriver_autoinstaller
import pandas as pd

def check_exists_by_xpath(xpath):
    try:
        driver.find_element_by_xpath(xpath)
    except NoSuchElementException:
        return False
    return True

def find_company(x,sep):
    try:
        return x.split(sep)[1]
    except:
        return ""
# input
location = "?geoUrn=%5B%22102454443%22%5D&"

# lists
ls_keyword = ["Client%20Advisor",
              'Personal%20Banker',
              'Offshore%20banker',
              'onshore%20banker',
              'Wealth%20Planning',
              'Financial%20Consultant']
ls_keyword_1 =["Consumer%20Banking",
                "Personal%20Banking",
                "Priority%20Banking",
                "Retail%20Banking",
                "Premier%20Banking"]
ls_filter = ["SME",
             "Institutional Banking",
             "Transaction banking",
             "Commercial",
             "Corporate", 
             "Investment Bank"]

# previous data
try:
    df_prev = pd.read_csv(r"C:\Users\JHui\Downloads\linkedin_1126.csv")
    ls_name = list(df_prev["name"])
    ls_title = list(df_prev["title"])
    ls_link = list(df_prev["link"].apply(lambda x: x.replace('=hyperlink("',"").replace('","link")','')))
except:
    ls_name = []
    ls_title = []
    ls_link = []

# xpath
xp_email = "/html/body/main/section[1]/div/div/form/div[2]/div[1]/input"
xp_pw = "/html/body/main/section[1]/div/div/form/div[2]/div[2]/input"
xp_login = "/html/body/main/section[1]/div/div/form/button"

start_time = time.time()

# start crawl    
chromedriver_autoinstaller.install()
driver = webdriver.Chrome()
time.sleep(5)
timeout = 60
  
# log in
driver.get("https://linkedin.com")
driver.find_element_by_xpath(xp_email).send_keys("")
driver.find_element_by_xpath(xp_pw).send_keys("")
driver.find_element_by_xpath(xp_login).click()  
for keyword in ls_keyword[1:]: 
    for keyword1 in ls_keyword_1:
        combined_keyword = keyword +"%20"+keyword1
        page_max = 50
        for p in range(1,page_max+1):
            start_time = time.time()
            
            driver.get("https://www.linkedin.com/search/results/people/%skeywords=%s&origin=FACETED_SEARCH&page=%d&sid=(bj"%(location,combined_keyword,p))
            try:
                num_res = driver.find_element_by_xpath("/html/body/div[5]/div[3]/div/div[2]/div/div[1]/main/div/div/div[1]").text
            except:
                num_res = driver.find_element_by_xpath("/html/body/div[6]/div[3]/div/div[2]/div/div[1]/main/div/div/div[1]").text
            num_res = int(num_res.replace("約有 ","").replace(",","").split(" ")[0])
            page = num_res//10
            time.sleep(7)
            if p != page:
                tbl_len = 10
            else:
                tbl_len = num_res%10
            # each page
            for ppl in range(1,tbl_len+1):
                ppl_filter = False
                try:
                    if driver.find_element_by_xpath("/html/body/div[6]/div[3]/div/div[2]/div/div[1]/main/div/div/div[2]/ul/li[%d]/div/div/div[2]/div[1]/div[1]/div/span/span/a"%ppl).text=="LinkedIn 會員":
                        continue
                except:
                    if driver.find_element_by_xpath("/html/body/div[5]/div[3]/div/div[2]/div/div[1]/main/div/div/div[2]/ul/li[%d]/div/div/div[2]/div[1]/div[1]/div/span/span/a"%ppl).text=="LinkedIn 會員":
                        continue
                try:
                    name = driver.find_element_by_xpath("/html/body/div[6]/div[3]/div/div[2]/div/div[1]/main/div/div/div[2]/ul/li[%d]/div/div/div[2]/div[1]/div[1]/div/span[1]/span/a/span/span[1]"%ppl).text                                 
                    title = driver.find_element_by_xpath("/html/body/div[6]/div[3]/div/div[2]/div/div[1]/main/div/div/div[2]/ul/li[%d]/div/div/div[2]/div[1]/div[2]/div/div[1]"%ppl).text
                    url = driver.find_element_by_xpath("/html/body/div[6]/div[3]/div/div[2]/div/div[1]/main/div/div/div[2]/ul/li[%d]/div/div/div[2]/div[1]/div[1]/div/span[1]/span/a"%ppl).get_attribute("href")
                except:
                    name = driver.find_element_by_xpath("/html/body/div[5]/div[3]/div/div[2]/div/div[1]/main/div/div/div[2]/ul/li[%d]/div/div/div[2]/div[1]/div[1]/div/span[1]/span/a/span/span[1]"%ppl).text                                 
                    title = driver.find_element_by_xpath("/html/body/div[5]/div[3]/div/div[2]/div/div[1]/main/div/div/div[2]/ul/li[%d]/div/div/div[2]/div[1]/div[2]/div/div[1]"%ppl).text
                    url = driver.find_element_by_xpath("/html/body/div[5]/div[3]/div/div[2]/div/div[1]/main/div/div/div[2]/ul/li[%d]/div/div/div[2]/div[1]/div[1]/div/span[1]/span/a"%ppl).get_attribute("href")
                for word in ls_filter:
                    if word in title:
                        ppl_filter = True
                        break
                if (url not in ls_link) and (ppl_filter==False):
                    ls_name.append(name)
                    ls_title.append(title)
                    ls_link.append(url)
            if (p == page_max) or (p==page):
                break
            
# create output
df_final = pd.DataFrame()
df_final["name"] = ls_name
df_final["title"] = ls_title
df_final["link"] = ls_link
df_final = df_final[df_final["title"].str.lower().str.contains("sme|institutional bank|transaction bank|commercial|corporate|investment iank")==False]
df_final["Company"] = df_final["title"].apply(lambda x: find_company(x," at "))

df_final["link"] = df_final["link"].apply(lambda x: '=hyperlink("'+x+'","link")')

df_final.to_csv(r"C:\Users\JHui\Downloads\linkedin_1202.csv")