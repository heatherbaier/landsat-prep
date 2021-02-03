#!/usr/bin/env python
# coding: utf-8

# In[24]:


from bs4 import BeautifulSoup
from glob import glob
import pandas as pd
import os, shutil
import datetime
import requests


# In[4]:


df = pd.read_csv("./data/1997-01-01-2021-01-24-Caribbean-Caucasus_and_Central_Asia-Central_America-East_Asia-Eastern_Africa-Europe-Middle_Africa-Middle_East-Northern_Africa-South_America-South_Asia-Southeast_Asia-Southern_Africa-Western_Afri copy.csv")

# Chemical Attacks
chemical = df.dropna(subset = ['notes'])
chemical = chemical[chemical['notes'].str.contains("chemical")]
print("Number of records mentioning 'chemical': ", chemical.shape[0])
chemical.head()


# In[39]:


# chemical.shape
# chemical.to_csv("./data/chemical_attacks.csv")


# ## Read in s3 scenes

# In[6]:


s3_scenes = pd.read_csv('http://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz', compression='gzip')


# In[7]:


s3_scenes.head()


# In[8]:


s3_scenes.columns


# ## Read in chemical attacks with path/rows

# In[22]:


final = pd.read_csv("./data/chemical_attacks_prs.csv")
final = final[0:500]
print(final.shape)
final.head()


# In[23]:


paths = final['PATH']
rows = final["ROW"]

bulk_list = []
count = 0

# Iterate through paths and rows
for path, row in zip(paths, rows):

    print(count, " out of ", len(paths))

    # Filter the Landsat Amazon S3 table for images matching path, row, cloudcover and processing state.
    scenes = s3_scenes[(s3_scenes.path == path) & (s3_scenes.row == row) & 
                       (s3_scenes.cloudCover > 0) & (s3_scenes.cloudCover <= 50) & 
                       (~s3_scenes.productId.str.contains('_T2')) &
                       (~s3_scenes.productId.str.contains('_RT'))]

    # Add the selected scene to the bulk download list.
    bulk_list.append(scenes)
    
    count += 1
        

        
bulk_frame = pd.concat(bulk_list, 1).T
bulk_frame = bulk_frame.T
#bulk_frame = bulk_frame.dropna(subset = ['productId'])
bulk_frame.head()

sorted(bulk_frame['acquisitionDate'])


# ## Clean scenes dataframe 

# In[25]:


tada = pd.DataFrame()

for i in bulk_list:
    if len(tada) == 0:
        tada = i
    else:
        tada = tada.append(i)
        
tada['acquisitionDate'] = tada['acquisitionDate'].str.split(" ").str[0]
tada['acquisitionDate'] = pd.to_datetime(tada['acquisitionDate'])
tada["PATH_ROW"] = tada["path"].astype(str) + "_" + tada["row"].astype(str)
tada.head()


# In[26]:


# tada.to_csv("./data/chemical_scenes[0:500].csv")


# ## Clean chemical terror dataframe with p/r's

# In[27]:


month_dict = {1:"January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June", 7:"July", 
              8:"August", 9:"September", 10:"October",11:"November",12:"December"}
month_dict = {v: k for k, v in month_dict.items()}

final["PATH_ROW"] = final["PATH"].astype(str) + "_" + final["ROW"].astype(str)
final["DAY"] = final["event_date"].str.split(" ").str[0]
final["MONTH"] = final["event_date"].str.split(" ").str[1].map(month_dict)
final["YEAR"] = final["event_date"].str.split(" ").str[2]
final["DATE"] = final["YEAR"].astype(str) + "/" + final["MONTH"].astype(str) + "/" + final["DAY"].astype(str)
final["DATE"] = pd.to_datetime(final["DATE"])
final['DATE_AFTER'] = final['DATE'] + datetime.timedelta(days = 3)
final.head()


zipped  = zip(final["PATH_ROW"], final["DATE"])
zipped_after  = zip(final["PATH_ROW"], final["DATE_AFTER"])


# In[28]:


final.head()


# In[29]:


def grab_time_pr(df, dic, dic_after):    
    to_return = pd.DataFrame()
    for i in range(0, len(dic)):
        cur = df[df['acquisitionDate'] >= dic[i][1]]
        cur = cur[cur['acquisitionDate'] <= dic_after[i][1]]
        cur = cur[cur['PATH_ROW'] == dic[i][0]]
        if(len(cur)) > 0:
            cur = cur[0:1]
            if len(to_return) == 0:
                to_return = cur
            else:
                to_return = to_return.append(cur)
    return to_return


# In[30]:


dta = grab_time_pr(tada, zipped, zipped_after)


# In[31]:


dta.shape


# In[32]:


dta


# In[34]:


with_info = pd.merge(dta, final, on = "PATH_ROW")
with_info.head()


# In[45]:


for i in range(0, len(with_info)):
    print(with_info['notes'][i])


# In[46]:


with_info.columns


# In[47]:


with_info['event_type'].unique()


# In[57]:


temp = with_info[with_info['event_type'].isin(['Violence against civilians', 'Riots', 'Battles', 'Strategic developments', 'Explosions/Remote violence'])]
          
          
for col, row in temp.iterrows():
    print(row.notes)
    print("\n")
          
temp.


# In[ ]:




