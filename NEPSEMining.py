
# coding: utf-8

# In[1]:


import requests
import urllib3
from bs4 import BeautifulSoup as BS
import pandas as pd
import time
import os.path

# In[2]:


company_listing = "http://www.nepalstock.com.np/company?_limit=500"


# In[3]:


http = urllib3.PoolManager()
http.addheaders = [('User-agent', 'Mozilla/61.0')]
web_page = http.request('GET',company_listing)
soup = BS(web_page.data, 'html5lib')
table = soup.find('table')
company=[]
rows = [row.findAll('td') for row in table.findAll('tr')[1:-2]]
col = 0
notfirstrun = False
for row in rows:
    companydata =[]
    for data in row:
        if col == 5 and notfirstrun:
            companydata.append(data.a.get('href').split('/')[-1])
        else:
            companydata.append(data.text.strip())
        col += 1
    company.append(companydata)
    col =0
    notfirstrun = True

df = pd.DataFrame(company[1:],columns=company[0])
df.rename(columns={'Operations':'Symbol No'},inplace=True)
df.index.name = "SN"
df.drop(columns='',inplace=True)
df.drop(columns='S.N.',inplace=True)
df.to_json('CompanyList.json',orient='index')
print('There are %s Companies'%len(df.index))
df.head()


# In[4]:


# Getting Company Details
symbol = "NMB"
url = "http://www.nepalstock.com/company/"
try:
    req = requests.post(url, data={"stock_symbol":symbol}, verify=False)
except requests.exceptions.RequestException as e:
    print(e)
response = req.text
soup = BS(response, "lxml")
table = soup.find("table")
#print ("Company: ",table.findAll("td")[0].string)
for row in table.findAll("tr")[4:]:
    col = row.findAll("td")
    #print (col[0].string,": ",col[1].string)


# # Daily FloorSheet Data

# In[5]:


DailyFloorSheet="http://www.nepalstock.com.np/main/floorsheet/index/0/?_limit=5000"

http = urllib3.PoolManager()
http.addheaders = [('User-agent', 'Mozilla/61.0')]
web_page = http.request('GET',DailyFloorSheet)
soup = BS(web_page.data, 'html5lib')
table = soup.find('table')
FloorSheet=[]
rows = [row.findAll('td') for row in table.findAll('tr')[1:-2]]
for row in rows:
    FloorSheet.append([data.text.strip() for data in row])
FloorSheetdf = pd.DataFrame(FloorSheet[1:-1],columns=FloorSheet[0])
FloorSheetdf.head()


# In[6]:


df['date'] = pd.to_datetime(FloorSheetdf['Contract No'],format='%Y%m%d%H%M%f').dt.date
df['date']


# # CompanyStocks Transactions

# In[16]:


def CompanyStocksTransactions(SymbolNo,startDate):
    url="http://www.nepalstock.com.np/company/transactions/%s/0/?startDate=%s&endDate=&_limit=9000000"%(SymbolNo,startDate)
    print("Connecting to %s "%url)
    http = urllib3.PoolManager()
    http.addheaders = [('User-agent', 'Mozilla/61.0')]
    web_page = http.request('GET',url)
    print("Adding to DataFrame")
    soup = BS(web_page.data, 'html5lib')
    table = soup.find('table')
    FloorSheet=[]
    rows = [row.findAll('td') for row in table.findAll('tr')[1:-2]]
    for row in rows:
        FloorSheet.append([data.text.strip() for data in row])
    FloorSheetdf = pd.DataFrame(FloorSheet[1:],columns=FloorSheet[0])
    #FloorSheetdf['Date']=pd.to_datetime(FloorSheetdf['Contract No'], format='%Y%m%d%H%M%f', errors='ignore')
    return FloorSheetdf


# In[17]:
def CompanyStocksTransactions2(SymbolNo,startDate,endDate):
    url=f"http://www.nepalstock.com.np/company/transactions/{SymbolNo}/0/?startDate={startDate}&endDate={endDate}&_limit=9000000"
    print("Connecting to %s "%url)
    http = urllib3.PoolManager()
    http.addheaders = [('User-agent', 'Mozilla/61.0')]
    web_page = http.request('GET',url)
    print("Parsing")
    soup = BS(web_page.data, 'html5lib')
    table = soup.find('table')    
    FloorSheet=[]
    rows = [row.findAll('td') for row in table.findAll('tr')[1:-2]]
    if rows == []:
        print(f"Empty between {startDate} and {endDate}")
        return None   
    print("Adding to DataFrame")
    for row in rows:
        FloorSheet.append([data.text.strip() for data in row])
    FloorSheetdf = pd.DataFrame(FloorSheet[1:],columns=FloorSheet[0])
    #FloorSheetdf['Date']=pd.to_datetime(FloorSheetdf['Contract No'], format='%Y%m%d%H%M%f', errors='ignore')
    return FloorSheetdf



startDate= '2001-1-1'
count = 0
os.mkdir("data")
for symbol in list(df['Symbol No']):
    print("Stock No: %s\nTime: %s"%(symbol,time.ctime()))
    filename = "./data/NEPSE%s.csv"%symbol
    count +=1
    if os.path.isfile(filename):
        print("Stock No: %s Data Already Downloaded"%symbol)
    else:
        dftest=CompanyStocksTransactions(symbol,startDate)
        dftest.to_csv(filename)
    print("%s of %s completed \n"%(count,len(df['Symbol No'])))



